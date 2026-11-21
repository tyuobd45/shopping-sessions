//! Exact, parallel Shopping Sessions processor. Uses only the Rust standard library.
//! Writes two intermediate CSVs; the Python entry point converts them to Parquet.
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

type Map<K,V> = HashMap<K,V,BuildHasherDefault<BlockHasher>>;
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

fn fields<const N:usize>(line:&[u8])->Option<[&[u8];N]> {
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
fn number(s:&[u8])->Option<u64> {
    if s.is_empty() {return None;}
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
                end=match bytes[end..].iter().position(|&c|c==b'\n') {Some(p)=>end+p+1,None=>bytes.len()};
            }
            out.push(&bytes[start..end]); start=end;
        }
    }
    out
}
struct Ids {names:Vec<Vec<u8>>, lookup:Map<Vec<u8>,u32>, parent:Vec<u32>, sizes:Vec<u32>, minimum:Vec<u32>}
impl Ids {
    fn new()->Self {Self{names:Vec::new(),lookup:Map::default(),parent:Vec::new(),sizes:Vec::new(),minimum:Vec::new()}}
    fn intern(&mut self,s:&[u8])->u32 {
        if let Some(&id)=self.lookup.get(s) {return id;}
        let id=self.names.len() as u32;
        self.names.push(s.to_vec()); self.lookup.insert(s.to_vec(),id);
        self.parent.push(id); self.sizes.push(1); self.minimum.push(id); id
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
        for v in self.lookup.values_mut() {*v=map[*v as usize];}
        self.parent.clear();self.sizes.clear();self.minimum.clear();
    }
}
fn link_ids(data:&Path)->io::Result<Ids> {
    let mut ids=Ids::new();
    for path in csv_paths(&data.join("links"))? {
        let bytes=fs::read(path)?;
        for line in body(&bytes).split(|&c|c==b'\n') {
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
    for line in body(&bytes).split(|&c|c==b'\n') {
        if let Some([a,b])=fields::<2>(line) {
            if a.is_empty(){continue;}
            let id=out.names.len() as u32;
            out.lookup.insert(a.to_vec(),id);out.names.push(a.to_vec());out.labels.push(b.to_vec());
        }
    }
    Ok(out)
}
#[derive(Clone,Copy)]
struct RawEvent<'a>{id:&'a[u8], ts:i64, ingest:i64, user:u32, category:u32, price:u32, valid:bool}
#[derive(Clone,Copy)]
struct Event{ts:i64,user:u32,category:u32,price:u32}
#[derive(Clone,Copy)]
struct Click<'a>{id:&'a[u8],ts:i64,user:u32,campaign:u32}
struct Parsed<'a>{events:Vec<Vec<RawEvent<'a>>>,clicks:Vec<Click<'a>>,unknown:Vec<&'a[u8]>}
fn local_user<'a>(s:&'a[u8],known:&Map<Vec<u8>,u32>,local:&mut Map<&'a[u8],u32>,names:&mut Vec<&'a[u8]>)->u32 {
    if let Some(&id)=known.get(s) {return id;}
    if let Some(&id)=local.get(s) {return id|UNKNOWN;}
    let id=names.len() as u32;names.push(s);local.insert(s,id);id|UNKNOWN
}
fn parse_parallel<'a>(event_tasks:&[&'a[u8]],click_tasks:&[&'a[u8]],known:&Map<Vec<u8>,u32>,
    products:&Map<Vec<u8>,u32>,campaigns:&Map<Vec<u8>,u32>,unknown_category:u32,
    lower:i64,upper:i64,ncpu:usize)->Vec<Parsed<'a>> {
    let queue=AtomicUsize::new(0);
    thread::scope(|scope| {
        let mut handles=Vec::new();
        for _ in 0..ncpu {
            let queue=&queue;
            handles.push(scope.spawn(move||{
                let mut out=Parsed{events:(0..ncpu).map(|_|Vec::new()).collect(),clicks:Vec::new(),unknown:Vec::new()};
                let mut local=Map::default();
                loop {
                    let job=queue.fetch_add(1,Ordering::Relaxed);
                    if job>=event_tasks.len()+click_tasks.len(){break;}
                    if job<event_tasks.len() {
                        for line in event_tasks[job].split(|&c|c==b'\n') {
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
                            let user=if valid {local_user(f[1],known,&mut local,&mut out.unknown)} else {0};
                            let category=if valid&&price.unwrap_or(0)>0 {*products.get(f[5]).unwrap_or(&unknown_category)} else {unknown_category};
                            let shard=(fingerprint(f[0]) as usize)%ncpu;
                            out.events[shard].push(RawEvent{id:f[0],ts:t,ingest,user,category,price:price.unwrap_or(0),valid});
                        }
                    } else {
                        for line in click_tasks[job-event_tasks.len()].split(|&c|c==b'\n') {
                            let Some(f)=fields::<4>(line) else {continue;};
                            if f[0].is_empty()||f[1].is_empty(){continue;}
                            let Some(&campaign)=campaigns.get(f[2]) else {continue;};
                            let Some(ts)=timestamp(f[3]) else {continue;};
                            if ts<lower-WEEK||ts>=upper {continue;}
                            let user=local_user(f[1],known,&mut local,&mut out.unknown);
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
fn dedup(parts:Vec<Vec<RawEvent<'_>>>)->Vec<Event> {
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
        if r.valid {result.push(Event{ts:r.ts,user:r.user,category:r.category,price:r.price});}
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
fn aggregate(mut events:Vec<Event>,mut clicks:Vec<Click<'_>>,categories:&[Vec<u8>],days:usize,base_day:i64,campaigns:usize)->Results {
    events.sort_unstable_by_key(|e|(e.user,e.ts));
    clicks.sort_unstable_by(|a,b|a.user.cmp(&b.user).then(a.ts.cmp(&b.ts)).then(a.id.cmp(b.id)));
    let mut out=Results{users:Vec::new(),campaigns:vec![CampaignDay::default();(campaigns+1)*days]};
    let mut ei=0;let mut ci=0;
    while ei<events.len() {
        let user=events[ei].user;
        let begin=ei;
        while ei<events.len()&&events[ei].user==user {ei+=1;}
        while ci<clicks.len()&&clicks[ci].user<user {ci+=1;}
        let cb=ci;
        while ci<clicks.len()&&clicks[ci].user==user {ci+=1;}
        let mut ads=Ads{clicks:&clicks[cb..ci],left:0,right:0,latest:0,active:Map::default()};
        let mut daily:Vec<UserDay>=(0..days).map(|_|UserDay::default()).collect();
        let mut start=events[begin].ts;
        let mut previous=start;
        let mut day=(start.div_euclid(DAY)-base_day) as usize;
        let mut revenue=0u64;
        daily[day].sessions+=1;
        for event in &events[begin..ei] {
            if event.ts-previous>1800000 {
                ads.session(start,day,revenue,days,campaigns as u32,&mut out.campaigns);
                start=event.ts;day=(start.div_euclid(DAY)-base_day) as usize;
                revenue=0;daily[day].sessions+=1;
            }
            previous=event.ts;
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
fn run()->Result<(),Box<dyn std::error::Error>> {
    let args:Vec<String>=std::env::args().collect();
    if args.len()!=6 {return Err("usage: fast-engine DATA WORK LOWER_MS UPPER_MS NCPU".into());}
    let data=Path::new(&args[1]);let work=Path::new(&args[2]);
    let lower:i64=args[3].parse()?;let upper:i64=args[4].parse()?;let ncpu:usize=args[5].parse::<usize>()?.clamp(1,8);
    let started=Instant::now();
    let mut ids=link_ids(data)?;
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
    let mut extra:Map<&[u8],u32>=Map::default();
    let mut mappings=Vec::with_capacity(parsed.len());
    for part in &parsed {
        let mapping:Vec<u32>=part.unknown.iter().map(|&name| {
            *extra.entry(name).or_insert_with(|| {
                let id=ids.names.len() as u32;ids.names.push(name.to_vec());id
            })
        }).collect();
        mappings.push(mapping);
    }
    drop(extra);
    let click_count:usize=parsed.iter().map(|p|p.clicks.len()).sum();
    let normalized=thread::scope(|scope| {
        let handles:Vec<_>=parsed.into_iter().zip(mappings).map(|(mut part,mapping)| {
            scope.spawn(move|| {
                for event in part.events.iter_mut().flatten() {
                    if event.user&UNKNOWN!=0 {event.user=mapping[(event.user&!UNKNOWN) as usize];}
                }
                for click in &mut part.clicks {
                    if click.user&UNKNOWN!=0 {click.user=mapping[(click.user&!UNKNOWN) as usize];}
                }
                part.unknown.clear();part
            })
        }).collect();
        handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>()
    });
    let mut shards:Vec<Vec<Vec<RawEvent<'_>>>>=(0..ncpu).map(|_|Vec::new()).collect();
    let mut click_shards:Vec<Vec<Click<'_>>>=(0..ncpu).map(|_|Vec::with_capacity(click_count/ncpu+1)).collect();
    for part in normalized {
        for (to,from) in shards.iter_mut().zip(part.events) {to.push(from);}
        for click in part.clicks {
            let shard=click.user as usize%ncpu;click_shards[shard].push(click);
        }
    }
    eprintln!("[native {:.2}s] input parsed",started.elapsed().as_secs_f64());
    let unique=thread::scope(|scope|{
        let handles:Vec<_>=shards.into_iter().map(|s|scope.spawn(move||dedup(s))).collect();
        handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>()
    });
    let mut event_shards:Vec<Vec<Event>>=(0..ncpu).map(|_|Vec::new()).collect();
    for part in unique {for event in part {event_shards[event.user as usize%ncpu].push(event);}}
    eprintln!("[native {:.2}s] event IDs deduplicated",started.elapsed().as_secs_f64());
    let base_day=lower.div_euclid(DAY);let days=((upper-1).div_euclid(DAY)-base_day+1) as usize;
    let results=thread::scope(|scope| {
        let handles:Vec<_>=event_shards.into_iter().zip(click_shards).map(|(e,c)| {
            let categories=&categories;let count=campaigns.names.len();
            scope.spawn(move||aggregate(e,c,categories,days,base_day,count))
        }).collect();
        handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>()
    });
    eprintln!("[native {:.2}s] sessions and campaigns aggregated",started.elapsed().as_secs_f64());
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
    fn fingerprint_collisions_keep_distinct_ids_and_earliest_invalid_copy() {
        let ids:[&[u8];3]=[b"a",b"a\0",b"a\0\0"];
        assert_eq!(fingerprint(ids[0]),fingerprint(ids[1]));
        assert_eq!(fingerprint(ids[0]),fingerprint(ids[2]));
        let row=|id,ingest,price,valid|RawEvent{id,ts:0,ingest,user:0,category:0,price,valid};
        let parts=vec![vec![row(ids[0],10,10,true),row(ids[1],20,20,true)],
                       vec![row(ids[1],5,0,false),row(ids[2],10,30,true)]];
        let mut prices:Vec<u32>=dedup(parts).iter().map(|e|e.price).collect();
        prices.sort_unstable();
        assert_eq!(prices,vec![10,30]);
    }
}
