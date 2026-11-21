//! Exact parallel Shopping Sessions processor with direct native Parquet output.
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;
use std::sync::Arc;
use bytes::Bytes;
use parquet::basic::{Compression,Encoding};
use parquet::data_type::{ByteArray,ByteArrayType,Int32Type,Int64Type};
use parquet::file::properties::{EnabledStatistics,WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

type Map<K,V> = HashMap<K,V,BuildHasherDefault<BlockHasher>>;
#[derive(Default)]
struct IdLookup {compact:Map<u64,u32>,short:Map<u128,u32>,long:Map<Vec<u8>,u32>}
const fn id_alphabet()->[u8;256] {
    let mut codes=[255u8;256];let mut i=0;
    while i<10 {codes[b'0' as usize+i]=i as u8;i+=1;}
    i=0;while i<26 {codes[b'a' as usize+i]=(10+i) as u8;codes[b'A' as usize+i]=(36+i) as u8;i+=1;}
    codes[b'_' as usize]=62;codes[b'-' as usize]=63;codes
}
const ID_ALPHABET:[u8;256]=id_alphabet();
impl IdLookup {
    fn with_capacity(count:usize)->Self {
        Self{compact:Map::with_capacity_and_hasher(count,Default::default()),short:Map::default(),long:Map::default()}
    }
    #[inline]
    fn encoded(value:&[u8])->Option<u64> {
        if value.len()>10 {return None;}
        let mut key=value.len() as u64;
        for &byte in value {
            let code=ID_ALPHABET[byte as usize];
            if code==255 {return None;}
            key=(key<<6)|code as u64;
        }
        Some(key)
    }
    #[inline]
    fn packed(value:&[u8])->u128 {
        let mut bytes=[0u8;16];bytes[0]=value.len() as u8;
        bytes[1..value.len()+1].copy_from_slice(value);
        u128::from_le_bytes(bytes)
    }
    #[inline]
    fn get(&self,value:&[u8])->Option<&u32> {
        if let Some(key)=Self::encoded(value) {self.compact.get(&key)}
        else if value.len()<=15 {self.short.get(&Self::packed(value))} else {self.long.get(value)}
    }
    #[cfg(test)]
    fn insert(&mut self,value:&[u8],id:u32) {
        if let Some(key)=Self::encoded(value) {self.compact.insert(key,id);}
        else if value.len()<=15 {self.short.insert(Self::packed(value),id);}
        else {self.long.insert(value.to_vec(),id);}
    }
    fn get_or_insert_with(&mut self,value:&[u8],create:impl FnOnce()->u32)->u32 {
        if let Some(key)=Self::encoded(value) {*self.compact.entry(key).or_insert_with(create)}
        else if value.len()<=15 {*self.short.entry(Self::packed(value)).or_insert_with(create)}
        else if let Some(&id)=self.long.get(value) {id}
        else {let id=create();self.long.insert(value.to_vec(),id);id}
    }
    fn clear(&mut self) {self.compact.clear();self.short.clear();self.long.clear();}
    fn shrink_to_fit(&mut self) {self.compact.shrink_to_fit();self.short.shrink_to_fit();self.long.shrink_to_fit();}
}
#[derive(Default)]
struct BlockHasher(u64);
impl Hasher for BlockHasher {
    fn finish(&self)->u64 {
        // Mix high and low bits independently of the partitioning bits.
        let mut h=self.0;
        h^=h>>33;h=h.wrapping_mul(0xff51afd7ed558ccd);
        h^=h>>33;h=h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h^(h>>33)
    }
    fn write(&mut self, mut bytes:&[u8]) {
        let mut h=self.0;
        while bytes.len()>=8 {
            let word=u64::from_le_bytes(bytes[..8].try_into().unwrap());
            h=(h.rotate_left(5)^word).wrapping_mul(0x517cc1b727220a95);
            bytes=&bytes[8..];
        }
        if !bytes.is_empty() {
            let mut word=0u64;
            for (i,&b) in bytes.iter().enumerate() { word|=(b as u64)<<(i*8); }
            h=(h.rotate_left(5)^word).wrapping_mul(0x517cc1b727220a95);
        }
        self.0=h;
    }
    fn write_u32(&mut self,v:u32) { self.0=(self.0.rotate_left(5)^v as u64).wrapping_mul(0x517cc1b727220a95); }
    fn write_usize(&mut self,v:usize) { self.write_u64(v as u64); }
    fn write_u64(&mut self,v:u64) { self.0=(self.0.rotate_left(5)^v).wrapping_mul(0x517cc1b727220a95); }
}
fn fingerprint(s:&[u8])->u64 { let mut h=BlockHasher::default(); h.write(s); h.finish() }
const DAY:i64=86_400_000;
const WEEK:i64=7*DAY;
const UNKNOWN:u32=1<<31;
const LOCAL_BITS:u32=28;
const LOCAL_MASK:u32=(1<<LOCAL_BITS)-1;

fn fields_scalar<const N:usize>(line:&[u8])->Option<[&[u8];N]> {
    let mut result=[&[][..];N];
    let mut split=line.split(|&c| c==b',');
    for slot in &mut result {
        let mut f=split.next()?;
        if f.first()==Some(&b'"') {
            if f.len()<2 || f.last()!=Some(&b'"') {return None;}
            f=&f[1..f.len()-1];
        }
        if f.contains(&b'"') { return None; }
        *slot=f;
    }
    if split.next().is_some() {return None;}
    Some(result)
}

#[cfg(all(target_arch="x86_64",target_feature="avx2"))]
fn fields_vector<const N:usize>(line:&[u8])->Option<[&[u8];N]> {
    use std::arch::x86_64::*;
    let mut commas=0u128;
    let mut quotes=0u128;
    let mut offset=0;
    // Every vector load stays within its source slice. The short final block
    // is copied into a zero-filled local array, so no mmap overread is possible.
    unsafe {
        let comma=_mm256_set1_epi8(b',' as i8);
        let quote=_mm256_set1_epi8(b'"' as i8);
        while offset+32<=line.len() {
            let block=_mm256_loadu_si256(line.as_ptr().add(offset).cast());
            commas|=(_mm256_movemask_epi8(_mm256_cmpeq_epi8(block,comma)) as u32 as u128)<<offset;
            quotes|=(_mm256_movemask_epi8(_mm256_cmpeq_epi8(block,quote)) as u32 as u128)<<offset;
            offset+=32;
        }
        if offset<line.len() {
            let mut tail=[0u8;32];
            tail[..line.len()-offset].copy_from_slice(&line[offset..]);
            let block=_mm256_loadu_si256(tail.as_ptr().cast());
            commas|=(_mm256_movemask_epi8(_mm256_cmpeq_epi8(block,comma)) as u32 as u128)<<offset;
            quotes|=(_mm256_movemask_epi8(_mm256_cmpeq_epi8(block,quote)) as u32 as u128)<<offset;
        }
    }
    if commas.count_ones() as usize!=N-1 {return None;}
    let mut out=[&[][..];N];
    let mut start=0;
    let mut expected_quotes=0u128;
    for (i,slot) in out.iter_mut().enumerate() {
        let end=if i==N-1 {line.len()} else {let p=commas.trailing_zeros() as usize;commas&=commas-1;p};
        if start<end&&line[start]==b'"' {
            if end-start<2||line[end-1]!=b'"' {return None;}
            expected_quotes|=(1u128<<start)|(1u128<<(end-1));
            *slot=&line[start+1..end-1];
        } else {*slot=&line[start..end];}
        start=end+1;
    }
    if quotes!=expected_quotes {return None;}
    Some(out)
}

#[inline]
fn fields<const N:usize>(line:&[u8])->Option<[&[u8];N]> {
    #[cfg(all(target_arch="x86_64",target_feature="avx2"))]
    if line.len()>=32&&line.len()<=128 {return fields_vector::<N>(line);}
    fields_scalar::<N>(line)
}

#[inline]
fn eight_digits(s:&[u8])->Option<u64> {
    let word=u64::from_le_bytes(s[..8].try_into().unwrap());
    if ((word.wrapping_add(0x4646464646464646)|word.wrapping_sub(0x3030303030303030))&0x8080808080808080)!=0 {return None;}
    let mut digits=word&0x0f0f0f0f0f0f0f0f;
    digits=(digits.wrapping_mul(10)+(digits>>8))&0x00ff00ff00ff00ff;
    digits=(digits.wrapping_mul(100)+(digits>>16))&0x0000ffff0000ffff;
    Some((digits.wrapping_mul(10000)+(digits>>32))&0xffffffff)
}

#[inline]
fn number(s:&[u8])->Option<u64> {
    if s.is_empty() {return None;}
    if s.len()<=16 {
        let (mut n,rest)=if s.len()>=8 {(eight_digits(s)?,&s[8..])} else {(0,s)};
        for &c in rest {if !c.is_ascii_digit(){return None;}n=n*10+(c-b'0') as u64;}
        return Some(n);
    }
    let mut n=0u64;
    for &c in s {
        if !c.is_ascii_digit() {return None;}
        n=n.checked_mul(10)?.checked_add((c-b'0') as u64)?;
    }
    Some(n)
}
fn two(s:&[u8],i:usize)->Option<i64> {
    let a=s[i]; let b=s[i+1];
    if !a.is_ascii_digit() || !b.is_ascii_digit() {return None;}
    Some(((a-b'0') as i64)*10+(b-b'0') as i64)
}
fn days_from_civil(mut y:i64,m:i64,d:i64)->i64 {
    y-=if m<=2 {1} else {0};
    let era=y.div_euclid(400);
    let yoe=y-era*400;
    let mp=m+if m>2 {-3} else {9};
    let doy=(153*mp+2)/5+d-1;
    era*146097+yoe*365+yoe/4-yoe/100+doy-719468
}
fn civil_from_days(day:i64)->(i64,i64,i64) {
    let z=day+719468;
    let era=z.div_euclid(146097);
    let doe=z-era*146097;
    let yoe=(doe-doe/1460+doe/36524-doe/146096)/365;
    let mut y=yoe+era*400;
    let doy=doe-(365*yoe+yoe/4-yoe/100);
    let mp=(5*doy+2)/153;
    let d=doy-(153*mp+2)/5+1;
    let m=mp+if mp<10 {3} else {-9};
    y+=if m<=2 {1} else {0};
    (y,m,d)
}
fn timestamp(s:&[u8])->Option<i64> {
    if !((s.len()==20 || s.len()==25) && s[4]==b'-') {
        let n=number(s)?;
        return i64::try_from(n).ok();
    }
    if s[7]!=b'-'||s[10]!=b'T'||s[13]!=b':'||s[16]!=b':' {return None;}
    let y=two(s,0)?*100+two(s,2)?;
    let m=two(s,5)?; let d=two(s,8)?;
    let h=two(s,11)?; let mi=two(s,14)?; let sec=two(s,17)?;
    if !(1..=12).contains(&m)||h>23||mi>59||sec>59 {return None;}
    let leap=y%4==0 && (y%100!=0||y%400==0);
    let md=match m {2=>if leap {29} else {28},4|6|9|11=>30,_=>31};
    if d<1||d>md {return None;}
    let mut t=days_from_civil(y,m,d)*DAY+(h*3600+mi*60+sec)*1000;
    if s.len()==20 {
        if s[19]!=b'Z' {return None;}
    } else {
        if (s[19]!=b'+'&&s[19]!=b'-')||s[22]!=b':' {return None;}
        let oh=two(s,20)?; let om=two(s,23)?;
        if oh>23||om>59 {return None;}
        let offset=(oh*60+om)*60000;
        if s[19]==b'+' {t-=offset;} else {t+=offset;}
    }
    Some(t)
}

fn lines(bytes:&[u8])->impl Iterator<Item=&[u8]> {
    let mut start=0;
    memchr::memchr_iter(b'\n',bytes).chain(std::iter::once(bytes.len())).map(move|end| {
        let line=&bytes[start..end];start=end+1;line
    })
}
fn csv_paths(folder:&Path)->io::Result<Vec<PathBuf>> {
    let mut out=Vec::new();
    if folder.exists() {
        for entry in fs::read_dir(folder)? {
            let path=entry?.path();
            if path.extension().is_some_and(|e| e=="csv") {out.push(path);}
        }
    }
    out.sort(); Ok(out)
}
fn body(bytes:&[u8])->&[u8] {
    match bytes.iter().position(|&c|c==b'\n') {Some(p)=>&bytes[p+1..],None=>&[]}
}
struct MappedFile {address:*mut std::ffi::c_void,length:usize}
unsafe extern "C" {
    fn mmap(address:*mut std::ffi::c_void,length:usize,protection:i32,flags:i32,fd:i32,offset:i64)->*mut std::ffi::c_void;
    fn munmap(address:*mut std::ffi::c_void,length:usize)->i32;
}
impl MappedFile {
    fn open(path:&Path)->io::Result<Self> {
        let file=File::open(path)?;
        let length=file.metadata()?.len() as usize;
        if length==0 {return Ok(Self{address:std::ptr::null_mut(),length});}
        // Read-only private mapping. Its slice is borrowed from this owner and
        // all scoped worker threads finish before the owner can be dropped.
        let address=unsafe{mmap(std::ptr::null_mut(),length,1,2,file.as_raw_fd(),0)};
        if address as isize == -1 {return Err(io::Error::last_os_error());}
        Ok(Self{address,length})
    }
    fn bytes(&self)->&[u8] {
        if self.length==0 {&[]} else {unsafe{std::slice::from_raw_parts(self.address.cast(),self.length)}}
    }
}
impl Drop for MappedFile {
    fn drop(&mut self) {if self.length!=0 {unsafe{munmap(self.address,self.length);}}}
}
fn load_files(folder:&Path)->io::Result<Vec<MappedFile>> {
    csv_paths(folder)?.iter().map(|p|MappedFile::open(p)).collect()
}
fn tasks(files:&[MappedFile])->Vec<&[u8]> {
    let mut out=Vec::new();
    for file in files {
        let bytes=body(file.bytes());
        let mut start=0;
        while start<bytes.len() {
            let mut end=(start+8*1024*1024).min(bytes.len());
            if end<bytes.len() {
                end=match memchr::memchr(b'\n',&bytes[end..]) {Some(p)=>end+p+1,None=>bytes.len()};
            }
            out.push(&bytes[start..end]); start=end;
        }
    }
    out
}
#[derive(Default)]
struct NameStore {data:Vec<u8>,ranges:Vec<(usize,usize)>}
impl NameStore {
    fn len(&self)->usize {self.ranges.len()}
    fn push(&mut self,name:&[u8]) {
        let start=self.data.len();self.data.extend_from_slice(name);self.ranges.push((start,self.data.len()));
    }
}
impl std::ops::Index<usize> for NameStore {
    type Output=[u8];
    fn index(&self,index:usize)->&[u8] {let(a,b)=self.ranges[index];&self.data[a..b]}
}
struct Ids {names:NameStore, lookup:IdLookup, parent:Vec<u32>, sizes:Vec<u32>, minimum:Vec<u32>}
impl Ids {
    fn new()->Self {Self{names:NameStore::default(),lookup:IdLookup::default(),parent:Vec::new(),sizes:Vec::new(),minimum:Vec::new()}}
    fn intern(&mut self,s:&[u8])->u32 {
        self.lookup.get_or_insert_with(s,|| {
            let id=self.names.len() as u32;
            self.names.push(s);self.parent.push(id);self.sizes.push(1);self.minimum.push(id);id
        })
    }
    fn find(&mut self,mut id:u32)->u32 {
        while self.parent[id as usize]!=id {
            self.parent[id as usize]=self.parent[self.parent[id as usize] as usize];
            id=self.parent[id as usize];
        }
        id
    }
    fn join(&mut self,a:&[u8],b:&[u8]) {
        let ai=self.intern(a);let bi=self.intern(b);
        let mut a=self.find(ai);let mut b=self.find(bi);
        if a==b {return;}
        if self.sizes[a as usize]<self.sizes[b as usize] {std::mem::swap(&mut a,&mut b);}
        self.parent[b as usize]=a; self.sizes[a as usize]+=self.sizes[b as usize];
        let ma=self.minimum[a as usize];let mb=self.minimum[b as usize];
        if self.names[mb as usize]<self.names[ma as usize] {self.minimum[a as usize]=mb;}
    }
    fn canonicalize(&mut self) {
        let map:Vec<u32>=(0..self.names.len()).map(|i|{let r=self.find(i as u32);self.minimum[r as usize]}).collect();
        for v in self.lookup.compact.values_mut().chain(self.lookup.short.values_mut()).chain(self.lookup.long.values_mut()) {*v=map[*v as usize];}
        self.parent.clear();self.sizes.clear();self.minimum.clear();
    }
}
fn link_ids(data:&Path)->io::Result<Ids> {
    let paths=csv_paths(&data.join("links"))?;
    let bytes:usize=paths.iter().map(|p|fs::metadata(p).map(|m|m.len() as usize)).collect::<io::Result<Vec<_>>>()?.into_iter().sum();
    let capacity=(bytes/16).min(8000000);
    let mut ids=Ids::new();ids.lookup=IdLookup::with_capacity(capacity);
    ids.names.data.reserve(bytes/2);ids.names.ranges.reserve(capacity);
    ids.parent.reserve(capacity);ids.sizes.reserve(capacity);ids.minimum.reserve(capacity);
    for path in paths {
        let bytes=fs::read(path)?;
        for line in lines(body(&bytes)) {
            if let Some([a,b])=fields::<2>(line) {
                if !a.is_empty()&&!b.is_empty() {ids.join(a,b);}
            }
        }
    }
    ids.canonicalize();Ok(ids)
}
struct Dimension {lookup:Map<Vec<u8>,u32>,names:Vec<Vec<u8>>,labels:Vec<Vec<u8>>}
fn dimension(path:&Path)->io::Result<Dimension> {
    let bytes=fs::read(path)?;
    let mut out=Dimension{lookup:Map::default(),names:Vec::new(),labels:Vec::new()};
    for line in lines(body(&bytes)) {
        if let Some([a,b])=fields::<2>(line) {
            if a.is_empty(){continue;}
            let id=out.names.len() as u32;
            out.lookup.insert(a.to_vec(),id);out.names.push(a.to_vec());out.labels.push(b.to_vec());
        }
    }
    Ok(out)
}
#[derive(Clone,Copy)]
struct RawEvent<'a>{id:&'a[u8], ingest:i64, ts:u32, user:u32, category:u32, price:u32}
#[derive(Clone,Copy)]
struct Event{key:u64,category:u32,price:u32}
impl Event {
    #[inline] fn user(&self)->u32 {(self.key>>32) as u32}
    #[inline] fn ts(&self,lower:i64)->i64 {lower+(self.key as u32) as i64}
}
#[derive(Clone,Copy)]
struct Click<'a>{id:&'a[u8],ts:i64,user:u32,campaign:u32}
struct Parsed<'a>{events:Vec<Vec<RawEvent<'a>>>,clicks:Vec<Click<'a>>,unknown:Vec<&'a[u8]>}
fn local_user<'a>(s:&'a[u8],known:&IdLookup,local:&mut IdLookup,names:&mut Vec<&'a[u8]>,worker:usize)->u32 {
    if let Some(&id)=known.get(s) {return id;}
    local.get_or_insert_with(s,|| {
        let id=u32::try_from(names.len()).expect("user count fits u32");
        assert!(id<=LOCAL_MASK,"per-worker user count exceeds compact index");names.push(s);id
    })|UNKNOWN|((worker as u32)<<LOCAL_BITS)
}
fn parse_parallel<'a>(event_tasks:&[&'a[u8]],click_tasks:&[&'a[u8]],known:&IdLookup,
    products:&Map<Vec<u8>,u32>,campaigns:&Map<Vec<u8>,u32>,unknown_category:u32,
    lower:i64,upper:i64,ncpu:usize)->Vec<Parsed<'a>> {
    let queue=AtomicUsize::new(0);
    let rows=event_tasks.iter().map(|t|t.len()).sum::<usize>()/64;
    let dedup_shards=ncpu.next_power_of_two()*16;
    let events_per_shard=rows/ncpu/dedup_shards+1;
    let clicks_per_worker=click_tasks.iter().map(|t|t.len()).sum::<usize>()/32/ncpu+1;
    let users_per_worker=(rows/ncpu/12).min(1000000);
    thread::scope(|scope| {
        let mut handles=Vec::new();
        for worker in 0..ncpu {
            let queue=&queue;
            handles.push(scope.spawn(move||{
                let mut out=Parsed{events:(0..dedup_shards).map(|_|Vec::with_capacity(events_per_shard)).collect(),
                    clicks:Vec::with_capacity(clicks_per_worker),unknown:Vec::with_capacity(users_per_worker/2)};
                let mut local=IdLookup::with_capacity(users_per_worker);
                loop {
                    let job=queue.fetch_add(1,Ordering::Relaxed);
                    if job>=event_tasks.len()+click_tasks.len(){break;}
                    if job<event_tasks.len() {
                        for line in lines(event_tasks[job]) {
                            let Some(f)=fields::<7>(line) else {continue;};
                            if f[0].is_empty(){continue;}
                            let ingest=timestamp(f[3]).expect("ingest_ts is valid F1 by specification");
                            let ts=timestamp(f[2]);
                            let kind=match f[4]{b"view"|b"cart"=>1,b"purchase"=>2,_=>0};
                            let mut valid=!f[1].is_empty()&&!f[5].is_empty()&&kind!=0;
                            let t=ts.unwrap_or(i64::MIN);
                            valid=valid&&ts.is_some()&&t>=lower&&t<upper;
                            // Avoid overflow for timestamps far outside the window.
                            if valid&&ingest.saturating_sub(t)>DAY {valid=false;}
                            let price=if kind==2 {number(f[6]).filter(|&n|n>=1&&n<=10000000).map(|n|n as u32)} else {Some(0)};
                            valid=valid&&price.is_some();
                            let user=if valid {local_user(f[1],known,&mut local,&mut out.unknown,worker)} else {u32::MAX};
                            let category=if valid&&price.unwrap_or(0)>0 {*products.get(f[5]).unwrap_or(&unknown_category)} else {unknown_category};
                            let hash=fingerprint(f[0]);let shard=(hash as usize)&(dedup_shards-1);
                            out.events[shard].push(RawEvent{id:f[0],ts:if valid {(t-lower) as u32} else {0},ingest,user,category,price:price.unwrap_or(0)});
                        }
                    } else {
                        for line in lines(click_tasks[job-event_tasks.len()]) {
                            let Some(f)=fields::<4>(line) else {continue;};
                            if f[0].is_empty()||f[1].is_empty(){continue;}
                            let Some(&campaign)=campaigns.get(f[2]) else {continue;};
                            let Some(ts)=timestamp(f[3]) else {continue;};
                            if ts<lower-WEEK||ts>=upper {continue;}
                            let user=local_user(f[1],known,&mut local,&mut out.unknown,worker);
                            out.clicks.push(Click{id:f[0],ts,user,campaign});
                        }
                    }
                }
                out
            }));
        }
        handles.into_iter().map(|h|h.join().unwrap()).collect()
    })
}
fn dedup(parts:Vec<Vec<RawEvent<'_>>>,mappings:&[Vec<u32>])->Vec<Event> {
    let count=parts.iter().map(Vec::len).sum();
    let mut index:Map<u64,usize>=Map::with_capacity_and_hasher(count,Default::default());
    let mut selected:Vec<&RawEvent<'_>>=Vec::with_capacity(count);
    let mut collisions:Map<&[u8],usize>=Map::default();
    for r in parts.iter().flatten() {
        match index.entry(fingerprint(r.id)) {
            std::collections::hash_map::Entry::Vacant(e)=>{
                e.insert(selected.len());selected.push(r);
            },
            std::collections::hash_map::Entry::Occupied(mut e)=>{
                let mut i=*e.get_mut();
                if selected[i].id!=r.id {
                    // Fingerprints are an acceleration, never an identity.
                    // Resolve every collision using the full original bytes.
                    i=*collisions.entry(r.id).or_insert_with(|| {
                        let i=selected.len();selected.push(r);i
                    });
                }
                if r.ingest<selected[i].ingest {selected[i]=r;}
            }
        }
    }
    drop(index);drop(collisions);
    // Selection order follows the source buffers, making this payload pass
    // sequential rather than millions of random reads through hash buckets.
    let mut result=Vec::with_capacity(selected.len());
    for r in selected {
        if r.user!=u32::MAX {
            let user=if r.user&UNKNOWN!=0 {mappings[((r.user&!UNKNOWN)>>LOCAL_BITS) as usize][(r.user&LOCAL_MASK) as usize]} else {r.user};
            result.push(Event{key:((user as u64)<<32)|r.ts as u64,category:r.category,price:r.price});
        }
    }
    result
}
#[derive(Clone,Copy,Default)]
struct CampaignDay{sessions:u64,purchasing:u64,revenue:u64,assisted:u64}
#[derive(Default)]
struct UserDay{sessions:u64,events:u64,revenue:u64,categories:Vec<(u32,u64)>}
struct UserRow{user:u32,day:usize,sessions:u64,events:u64,revenue:u64,category:Option<u32>}
struct Results{users:Vec<UserRow>,campaigns:Vec<CampaignDay>}
struct Ads<'a>{clicks:&'a[Click<'a>],left:usize,right:usize,latest:u32,active:Map<u32,u32>}
impl<'a> Ads<'a> {
    fn session(&mut self,start:i64,day:usize,revenue:u64,days:usize,organic:u32,totals:&mut[CampaignDay]) {
        while self.right<self.clicks.len()&&self.clicks[self.right].ts<=start {
            let c=self.clicks[self.right];
            if self.right==0||c.ts!=self.clicks[self.right-1].ts {self.latest=c.campaign;}
            *self.active.entry(c.campaign).or_default()+=1;
            self.right+=1;
        }
        while self.left<self.right&&self.clicks[self.left].ts<start-WEEK {
            let campaign=self.clicks[self.left].campaign;
            let n=self.active.get_mut(&campaign).unwrap();*n-=1;
            if *n==0 {self.active.remove(&campaign);}
            self.left+=1;
        }
        let credited=if self.left<self.right {self.latest} else {organic};
        let t=&mut totals[credited as usize*days+day];
        t.sessions+=1;t.purchasing+=u64::from(revenue>0);t.revenue+=revenue;
        for &campaign in self.active.keys() {totals[campaign as usize*days+day].assisted+=1;}
    }
}
fn sort_events(events:&mut Vec<Event>) {
    if events.len()<2048 {events.sort_unstable_by_key(|e|e.key);return;}
    let significant=events.iter().fold(0u64,|bits,e|bits|e.key);
    let bits=64-significant.leading_zeros();
    let mut source=std::mem::take(events);
    let mut target=vec![Event{key:0,category:0,price:0};source.len()];
    for shift in (0..bits).step_by(11) {
        let mut counts=[0usize;2048];
        for event in &source {counts[((event.key>>shift)&2047) as usize]+=1;}
        if counts.iter().any(|&n|n==source.len()) {continue;}
        let mut offset=0;
        for count in &mut counts {let n=*count;*count=offset;offset+=n;}
        for &event in &source {
            let next=&mut counts[((event.key>>shift)&2047) as usize];
            target[*next]=event;*next+=1;
        }
        std::mem::swap(&mut source,&mut target);
    }
    *events=source;
}
fn aggregate(mut events:Vec<Event>,mut clicks:Vec<Click<'_>>,categories:&[Vec<u8>],days:usize,base_day:i64,campaigns:usize,lower:i64)->Results {
    sort_events(&mut events);
    clicks.sort_unstable_by(|a,b|a.user.cmp(&b.user).then(a.ts.cmp(&b.ts)).then(a.id.cmp(b.id)));
    let mut out=Results{users:Vec::new(),campaigns:vec![CampaignDay::default();(campaigns+1)*days]};
    let mut ei=0;let mut ci=0;
    while ei<events.len() {
        let user=events[ei].user();
        let begin=ei;
        while ei<events.len()&&events[ei].user()==user {ei+=1;}
        while ci<clicks.len()&&clicks[ci].user<user {ci+=1;}
        let cb=ci;
        while ci<clicks.len()&&clicks[ci].user==user {ci+=1;}
        let mut ads=Ads{clicks:&clicks[cb..ci],left:0,right:0,latest:0,active:Map::default()};
        let mut daily:Vec<UserDay>=(0..days).map(|_|UserDay::default()).collect();
        let mut start=events[begin].ts(lower);
        let mut previous=start;
        let mut day=(start.div_euclid(DAY)-base_day) as usize;
        let mut revenue=0u64;
        daily[day].sessions+=1;
        for event in &events[begin..ei] {
            let ts=event.ts(lower);
            if ts-previous>1800000 {
                ads.session(start,day,revenue,days,campaigns as u32,&mut out.campaigns);
                start=ts;day=(start.div_euclid(DAY)-base_day) as usize;
                revenue=0;daily[day].sessions+=1;
            }
            previous=ts;
            let d=&mut daily[day];d.events+=1;d.revenue+=event.price as u64;
            revenue+=event.price as u64;
            if event.price>0 {
                if let Some((_,n))=d.categories.iter_mut().find(|(c,_)|*c==event.category) {*n+=event.price as u64;}
                else {d.categories.push((event.category,event.price as u64));}
            }
        }
        ads.session(start,day,revenue,days,campaigns as u32,&mut out.campaigns);
        for (day,d) in daily.into_iter().enumerate() {
            if d.sessions==0 {continue;}
            let category=d.categories.iter().max_by(|(ca,na),(cb,nb)|na.cmp(nb).then_with(||categories[*cb as usize].cmp(&categories[*ca as usize]))).map(|&(c,_)|c);
            out.users.push(UserRow{user,day,sessions:d.sessions,events:d.events,revenue:d.revenue,category});
        }
    }
    out
}
fn quoted(w:&mut impl Write,s:&[u8])->io::Result<()> {
    w.write_all(b"\"")?;w.write_all(s)?;w.write_all(b"\"")
}
enum ColumnValues {
    Text(Vec<ByteArray>,Option<Vec<i16>>),
    Date(Vec<i32>),
    Integer(Vec<i64>),
}
fn write_columns(writer:&mut SerializedFileWriter<File>,columns:Vec<ColumnValues>)->parquet::errors::Result<()> {
    let mut group=writer.next_row_group()?;
    for values in columns {
        let mut column=group.next_column()?.expect("fixed output schema");
        match values {
            ColumnValues::Text(values,levels)=>{column.typed::<ByteArrayType>().write_batch(&values,levels.as_deref(),None)?;},
            ColumnValues::Date(values)=>{column.typed::<Int32Type>().write_batch(&values,None,None)?;},
            ColumnValues::Integer(values)=>{column.typed::<Int64Type>().write_batch(&values,None,None)?;},
        }
        column.close()?;
    }
    group.close()?;Ok(())
}
fn parquet_properties()->Arc<WriterProperties> {
    Arc::new(WriterProperties::builder().set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false).set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None).build())
}
fn write_user_parquet(path:PathBuf,rows:Vec<UserRow>,names:&[ByteArray],categories:&[ByteArray],base_day:i64)->parquet::errors::Result<()> {
    let schema=Arc::new(parse_message_type("message user_daily {
        REQUIRED BYTE_ARRAY user_id (UTF8); REQUIRED INT32 day (DATE);
        REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents;
        OPTIONAL BYTE_ARRAY top_category (UTF8); }")?);
    let mut writer=SerializedFileWriter::new(File::create(path)?,schema,parquet_properties())?;
    for chunk in rows.chunks(65536) {
        let mut users=Vec::with_capacity(chunk.len());let mut dates=Vec::with_capacity(chunk.len());
        let mut sessions=Vec::with_capacity(chunk.len());let mut events=Vec::with_capacity(chunk.len());
        let mut revenue=Vec::with_capacity(chunk.len());let mut top=Vec::new();let mut levels=Vec::with_capacity(chunk.len());
        for r in chunk {
            users.push(names[r.user as usize].clone());dates.push((base_day+r.day as i64) as i32);
            sessions.push(r.sessions as i64);events.push(r.events as i64);revenue.push(r.revenue as i64);
            if let Some(c)=r.category {levels.push(1);top.push(categories[c as usize].clone());} else {levels.push(0);}
        }
        write_columns(&mut writer,vec![ColumnValues::Text(users,None),ColumnValues::Date(dates),
            ColumnValues::Integer(sessions),ColumnValues::Integer(events),ColumnValues::Integer(revenue),ColumnValues::Text(top,Some(levels))])?;
    }
    writer.close()?;Ok(())
}
fn write_direct_parquet(output:&Path,results:Vec<Results>,names:NameStore,categories:Vec<Vec<u8>>,
    campaigns:Dimension,base_day:i64,days:usize)->parquet::errors::Result<()> {
    fs::create_dir_all(output.join("user_daily"))?;fs::create_dir_all(output.join("campaign_daily"))?;
    let arena=Bytes::from(names.data);
    let names:Vec<ByteArray>=names.ranges.into_iter().map(|(a,b)|ByteArray::from(arena.slice(a..b))).collect();
    let categories:Vec<ByteArray>=categories.into_iter().map(ByteArray::from).collect();
    let mut total=vec![CampaignDay::default();(campaigns.names.len()+1)*days];
    let mut rows=Vec::new();
    for result in results {
        rows.push(result.users);
        for (to,from) in total.iter_mut().zip(result.campaigns) {
            to.sessions+=from.sessions;to.purchasing+=from.purchasing;to.revenue+=from.revenue;to.assisted+=from.assisted;
        }
    }
    thread::scope(|scope|->parquet::errors::Result<()> {
        let mut handles=Vec::new();
        for (part,rows) in rows.into_iter().enumerate() {
            let path=output.join("user_daily").join(format!("part-{part}.parquet"));
            let names=&names;let categories=&categories;
            handles.push(scope.spawn(move||write_user_parquet(path,rows,names,categories,base_day)));
        }
        for h in handles {h.join().expect("Parquet worker failed")?;}
        Ok(())
    })?;
    let schema=Arc::new(parse_message_type("message campaign_daily {
        REQUIRED BYTE_ARRAY campaign_id (UTF8); REQUIRED BYTE_ARRAY channel (UTF8);
        REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions;
        REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }")?);
    let mut writer=SerializedFileWriter::new(File::create(output.join("campaign_daily/part-0.parquet"))?,schema,parquet_properties())?;
    let mut ids=Vec::new();let mut channels=Vec::new();let mut dates=Vec::new();let mut sessions=Vec::new();
    let mut purchasing=Vec::new();let mut revenue=Vec::new();let mut assisted=Vec::new();
    for (campaign,chunk) in total.chunks(days).enumerate() {
        let (id,channel)=if campaign==campaigns.names.len() {(ByteArray::from(b"organic".to_vec()),ByteArray::from(b"organic".to_vec()))}
            else {(ByteArray::from(campaigns.names[campaign].clone()),ByteArray::from(campaigns.labels[campaign].clone()))};
        for (day,r) in chunk.iter().enumerate() {
            if r.sessions==0&&r.assisted==0 {continue;}
            ids.push(id.clone());channels.push(channel.clone());dates.push((base_day+day as i64) as i32);
            sessions.push(r.sessions as i64);purchasing.push(r.purchasing as i64);revenue.push(r.revenue as i64);assisted.push(r.assisted as i64);
        }
    }
    if !ids.is_empty() {write_columns(&mut writer,vec![ColumnValues::Text(ids,None),ColumnValues::Text(channels,None),ColumnValues::Date(dates),
        ColumnValues::Integer(sessions),ColumnValues::Integer(purchasing),ColumnValues::Integer(revenue),ColumnValues::Integer(assisted)])?;}
    writer.close()?;Ok(())
}
fn run()->Result<(),Box<dyn std::error::Error>> {
    let args:Vec<String>=std::env::args().collect();
    let legacy=args.len()==6;
    let (data,work,lower,upper,ncpu)=if legacy {
        (PathBuf::from(&args[1]),PathBuf::from(&args[2]),args[3].parse::<i64>()?,args[4].parse::<i64>()?,args[5].parse::<usize>()?.clamp(1,8))
    } else {
        let data=PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_|args.get(1).cloned().unwrap_or("/data".into())));
        let output=PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_|args.get(2).cloned().unwrap_or("/output".into())));
        let manifest:serde_json::Value=serde_json::from_slice(&fs::read(data.join("manifest.json"))?)?;
        let lower=timestamp(manifest["window_start"].as_str().ok_or("missing window_start")?.as_bytes()).ok_or("invalid window_start")?;
        let upper=timestamp(manifest["window_end"].as_str().ok_or("missing window_end")?.as_bytes()).ok_or("invalid window_end")?;
        let ncpu=std::env::var("NCPU").ok().and_then(|n|n.parse::<usize>().ok()).unwrap_or(8).clamp(1,8);
        (data,output,lower,upper,ncpu)
    };
    // The documented generated window is fourteen days. Relative milliseconds
    // preserve every timestamp exactly while keeping a row's sort key compact.
    if upper<=lower||upper-lower>u32::MAX as i64 {return Err("event window must fit 32-bit millisecond offsets".into());}
    let started=Instant::now();
    let mut ids=link_ids(&data)?;
    let products=dimension(&data.join("products.csv"))?;
    let campaigns=dimension(&data.join("campaigns.csv"))?;
    let mut categories:Vec<Vec<u8>>=Vec::new();let mut category_ids=Map::default();
    let mut product_categories=Map::default();
    for (name,label) in products.names.iter().zip(products.labels.iter()) {
        let label=if label.is_empty(){b"unknown".as_slice()}else{label.as_slice()};
        let next=categories.len() as u32;
        let id=*category_ids.entry(label.to_vec()).or_insert_with(||{categories.push(label.to_vec());next});
        product_categories.insert(name.clone(),id);
    }
    let unknown_category=match category_ids.get(b"unknown".as_slice()) {Some(&n)=>n,None=>{let n=categories.len() as u32;categories.push(b"unknown".to_vec());n}};
    let event_files=load_files(&data.join("events"))?;
    let click_files=load_files(&data.join("clicks"))?;
    let et=tasks(&event_files);let ct=tasks(&click_files);
    eprintln!("[native {:.2}s] IDs and input loaded",started.elapsed().as_secs_f64());
    let parsed=parse_parallel(&et,&ct,&ids.lookup,&product_categories,&campaigns.lookup,unknown_category,lower,upper,ncpu);
    eprintln!("[native {:.2}s] parallel parsing finished",started.elapsed().as_secs_f64());
    // Unknown IDs are absent from the immutable linked-ID map by definition.
    // Intern only those IDs in a small borrowed-key map, avoiding millions of
    // repeated lookups and allocations in the much larger alias dictionary.
    ids.lookup.clear();ids.lookup.shrink_to_fit();
    let unknown_count=parsed.iter().map(|p|p.unknown.len()).sum::<usize>();
    ids.names.ranges.reserve(unknown_count);ids.names.data.reserve(unknown_count*10);
    let mut extra=IdLookup::with_capacity(unknown_count);
    let mut mappings=Vec::with_capacity(parsed.len());
    for part in &parsed {
        let mapping:Vec<u32>=part.unknown.iter().map(|&name| {
            extra.get_or_insert_with(name,|| {let id=ids.names.len() as u32;ids.names.push(name);id})
        }).collect();
        mappings.push(mapping);
    }
    drop(extra);
    let click_count:usize=parsed.iter().map(|p|p.clicks.len()).sum();
    let normalized=thread::scope(|scope| {
        let handles:Vec<_>=parsed.into_iter().zip(&mappings).map(|(mut part,mapping)| {
            scope.spawn(move|| {
                for click in &mut part.clicks {
                    if click.user&UNKNOWN!=0 {click.user=mapping[(click.user&LOCAL_MASK) as usize];}
                }
                part.unknown.clear();part
            })
        }).collect();
        handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>()
    });
    let mut shards:Vec<Vec<Vec<RawEvent<'_>>>>=(0..ncpu.next_power_of_two()*16).map(|_|Vec::new()).collect();
    let mut click_shards:Vec<Vec<Click<'_>>>=(0..ncpu).map(|_|Vec::with_capacity(click_count/ncpu+1)).collect();
    for part in normalized {
        for (to,from) in shards.iter_mut().zip(part.events) {to.push(from);}
        for click in part.clicks {
            let shard=click.user as usize%ncpu;click_shards[shard].push(click);
        }
    }
    eprintln!("[native {:.2}s] input parsed",started.elapsed().as_secs_f64());
    let mut dedup_jobs:Vec<Vec<Vec<Vec<RawEvent<'_>>>>>=(0..ncpu).map(|_|Vec::new()).collect();
    for (i,shard) in shards.into_iter().enumerate() {dedup_jobs[i%ncpu].push(shard);}
    let unique=thread::scope(|scope|{
        let handles:Vec<_>=dedup_jobs.into_iter().map(|jobs| {
            let mappings=&mappings;
            scope.spawn(move|| {
                let capacity=jobs.iter().flatten().map(Vec::len).sum();
                let mut events=Vec::with_capacity(capacity);
                for job in jobs {events.extend(dedup(job,mappings));}
                events
            })
        }).collect();
        handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>()
    });
    let event_capacity=unique.iter().map(Vec::len).sum::<usize>()/ncpu;
    let mut event_shards:Vec<Vec<Event>>=(0..ncpu).map(|_|Vec::with_capacity(event_capacity+event_capacity/2+1)).collect();
    for part in unique {for event in part {event_shards[event.user() as usize%ncpu].push(event);}}
    eprintln!("[native {:.2}s] event IDs deduplicated",started.elapsed().as_secs_f64());
    let base_day=lower.div_euclid(DAY);let days=((upper-1).div_euclid(DAY)-base_day+1) as usize;
    let results=thread::scope(|scope| {
        let handles:Vec<_>=event_shards.into_iter().zip(click_shards).map(|(e,c)| {
            let categories=&categories;let count=campaigns.names.len();
            scope.spawn(move||aggregate(e,c,categories,days,base_day,count,lower))
        }).collect();
        handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>()
    });
    eprintln!("[native {:.2}s] sessions and campaigns aggregated",started.elapsed().as_secs_f64());
    if !legacy {
        write_direct_parquet(&work,results,ids.names,categories,campaigns,base_day,days)?;
        eprintln!("[native {:.2}s] Parquet output complete",started.elapsed().as_secs_f64());
        return Ok(());
    }
    let dates:Vec<String>=(0..days).map(|i|{let(y,m,d)=civil_from_days(base_day+i as i64);format!("{y:04}-{m:02}-{d:02}")}).collect();
    let mut users=BufWriter::with_capacity(1024*1024,File::create(work.join("user_daily.csv"))?);
    writeln!(users,"user_id,day,sessions,events,revenue_cents,top_category")?;
    let mut total=vec![CampaignDay::default();(campaigns.names.len()+1)*days];
    for result in results {
        for r in result.users {
            quoted(&mut users,&ids.names[r.user as usize])?;
            write!(users,",{},{},{},{},",dates[r.day],r.sessions,r.events,r.revenue)?;
            if let Some(c)=r.category {quoted(&mut users,&categories[c as usize])?;}
            users.write_all(b"\n")?;
        }
        for (to,from) in total.iter_mut().zip(result.campaigns) {
            to.sessions+=from.sessions;to.purchasing+=from.purchasing;to.revenue+=from.revenue;to.assisted+=from.assisted;
        }
    }
    users.flush()?;
    let mut out=BufWriter::with_capacity(1024*1024,File::create(work.join("campaign_daily.csv"))?);
    writeln!(out,"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions")?;
    for (campaign,chunk) in total.chunks(days).enumerate() {
        for (day,r) in chunk.iter().enumerate() {
            if r.sessions==0&&r.assisted==0 {continue;}
            let (name,channel)=if campaign==campaigns.names.len() {(b"organic".as_slice(),b"organic".as_slice())}
                              else {(campaigns.names[campaign].as_slice(),campaigns.labels[campaign].as_slice())};
            quoted(&mut out,name)?;out.write_all(b",")?;quoted(&mut out,channel)?;
            writeln!(out,",{},{},{},{},{}",dates[day],r.sessions,r.purchasing,r.revenue,r.assisted)?;
        }
    }
    out.flush()?;
    eprintln!("[native {:.2}s] output CSVs written",started.elapsed().as_secs_f64());
    Ok(())
}
fn main() {
    if let Err(error)=run() {eprintln!("fast-engine failed: {error}");std::process::exit(1);}
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn newline_iterator_matches_lf_only_split() {
        for input in [b"".as_slice(),b"a",b"a\n",b"\n",b"a\r\nb\rc",b"a\n\nb\n",b"\0\n\0"] {
            assert_eq!(lines(input).collect::<Vec<_>>(),input.split(|&b|b==b'\n').collect::<Vec<_>>());
        }
        for len in 0..260 {
            let input:Vec<u8>=(0..len).map(|i|((i*47+len*17)%256) as u8).collect();
            assert_eq!(lines(&input).collect::<Vec<_>>(),input.split(|&b|b==b'\n').collect::<Vec<_>>());
        }
    }
    #[test]
    fn radix_sort_preserves_full_width_keys_and_every_payload() {
        for len in [0usize,1,2047,2048,5000,10001] {
            for mode in 0..4 {
                let mut seed=0xc214862af783e19bu64;
                let mut values:Vec<Event>=(0..len).map(|i| {
                    seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;
                    let key=match mode {0=>seed,1=>seed&0x1fffffffffffff,2=>seed%8,_=>0};
                    Event{key,category:i as u32,price:(i*31) as u32}
                }).collect();
                let mut expected:Vec<_>=values.iter().map(|e|(e.key,e.category,e.price)).collect();
                expected.sort_unstable();sort_events(&mut values);
                assert!(values.windows(2).all(|w|w[0].key<=w[1].key));
                let mut actual:Vec<_>=values.iter().map(|e|(e.key,e.category,e.price)).collect();
                actual.sort_unstable();assert_eq!(actual,expected);
            }
        }
    }
    #[test]
    fn deferred_user_mapping_keeps_worker_indices_separate() {
        let mappings:Vec<Vec<u32>>=(0..8).map(|i|vec![10+i,100+i]).collect();
        let names:[&[u8];8]=[b"e0",b"e1",b"e2",b"e3",b"e4",b"e5",b"e6",b"e7"];
        let rows:Vec<_>=names.iter().enumerate().map(|(worker,&id)|RawEvent {
            id,ingest:0,ts:worker as u32,user:UNKNOWN|((worker as u32)<<LOCAL_BITS)|1,category:0,price:1
        }).collect();
        let mut users:Vec<_>=dedup(vec![rows],&mappings).iter().map(Event::user).collect();
        users.sort_unstable();assert_eq!(users,(100..108).collect::<Vec<_>>());
    }
    #[test]
    fn short_id_keys_preserve_length_bytes_and_long_ids() {
        let values:[&[u8];9]=[b"a",b"a\0",b"a\0\0",b"123456789012345",b"1234567890123456",b" A\tB\r",b"A",b"-_",b"0000000000"];
        let mut map=IdLookup::default();
        for (i,&value) in values.iter().enumerate() {map.insert(value,i as u32);}
        assert_eq!(map.compact.len()+map.short.len()+map.long.len(),values.len());
        for (i,&value) in values.iter().enumerate() {assert_eq!(map.get(value),Some(&(i as u32)));}
    }
    #[test]
    fn decimal_swar_rejects_every_non_ascii_digit_in_every_lane() {
        for lane in 0..8 {
            for byte in 0u16..256 {
                let mut s=*b"11111111";s[lane]=byte as u8;
                let expected=if s.iter().all(u8::is_ascii_digit) {
                    Some(std::str::from_utf8(&s).unwrap().parse::<u64>().unwrap())
                } else {None};
                assert_eq!(number(&s),expected,"lane={lane} byte={byte}");
            }
        }
        for s in ["00000000000000000000000000001791071890000", "18446744073709551615",
                  "18446744073709551616", "1791071890000.0", "-5", "２５０"] {
            let expected=if !s.is_empty()&&s.bytes().all(|c|c.is_ascii_digit()) {s.parse::<u64>().ok()} else {None};
            assert_eq!(number(s.as_bytes()),expected);
        }
    }
    #[cfg(all(target_arch="x86_64",target_feature="avx2"))]
    #[test]
    fn vector_csv_matches_scalar_at_vector_boundaries_and_invalid_quotes() {
        fn check<const N:usize>() {
            let base=vec!["a";N].join(",").into_bytes();
            for size in [31usize,32,33,63,64,65,95,96,97,127,128,129,160] {
                let mut line=vec![b'x';size-base.len()];line.extend_from_slice(&base);
                assert_eq!(fields::<N>(&line),fields_scalar::<N>(&line));
                for pos in 0..line.len() {
                    let saved=line[pos];
                    for byte in [b'"',b',',b'\r',b' ',0,255] {
                        line[pos]=byte;
                        assert_eq!(fields::<N>(&line),fields_scalar::<N>(&line),"size={size} pos={pos}");
                    }
                    line[pos]=saved;
                }
            }
        }
        check::<7>();check::<4>();check::<2>();
    }
    #[test]
    fn fingerprint_collisions_keep_distinct_ids_and_earliest_invalid_copy() {
        let ids:[&[u8];3]=[b"a",b"a\0",b"a\0\0"];
        assert_eq!(fingerprint(ids[0]),fingerprint(ids[1]));
        assert_eq!(fingerprint(ids[0]),fingerprint(ids[2]));
        let row=|id,ingest,price,valid|RawEvent{id,ts:0,ingest,user:if valid {0} else {u32::MAX},category:0,price};
        let parts=vec![vec![row(ids[0],10,10,true),row(ids[1],20,20,true)],
                       vec![row(ids[1],5,0,false),row(ids[2],10,30,true)]];
        let mut prices:Vec<u32>=dedup(parts,&[]).iter().map(|e|e.price).collect();
        prices.sort_unstable();
        assert_eq!(prices,vec![10,30]);
    }
}
