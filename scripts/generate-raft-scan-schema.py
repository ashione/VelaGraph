import re,pathlib
root=pathlib.Path(__file__).resolve().parents[1]
messages=[]
for file,prefix in [('internal_managed.proto','m'),('internal_raft.proto','r')]:
 s=(root/'crates/d2-raft/proto'/file).read_text()
 for match in re.finditer(r'\bmessage\s+(\w+)\s*\{',s):
  start=match.end();depth=1;i=start
  while depth: depth+=(s[i]=='{')-(s[i]=='}');i+=1
  body=s[start:i-1];oneof='oneof' in body
  fields=[]
  for m in re.finditer(r'(?:(repeated|optional)\s+)?([\w.]+)\s+\w+\s*=\s*(\d+)\s*;',body):
   rep,typ,tag=m.groups();fields.append((int(tag),typ,rep=='repeated',oneof and body.index(m.group())>body.index('oneof')))
  messages.append((prefix,match.group(1),fields))
lookup={(p,n):i for i,(p,n,_) in enumerate(messages)}
lines=[]
for p,n,fields in messages:
 row=[]
 for tag,t,rep,oneof in fields:
  key=('m',t.split('.')[-1]) if '.' in t else (p,t)
  if key in lookup: kind=f'Kind::Child({lookup[key]})'
  elif t in ['bytes','string']:kind='Kind::Bytes'
  elif t=='bool':kind='Kind::Bool'
  else:kind='Kind::Varint'
  row.append(f'Field{{tag:{tag},kind:{kind},repeated:{str(rep).lower()},oneof:{str(oneof).lower()}}}')
 lines.append(' &['+','.join(row)+'], // '+p+n)
sizes=[]
for prefix,name,_ in messages:
 sizes.append(f'std::mem::size_of::<{"crate::managed_rpc::grpc_codec::pb" if prefix=="m" else "wire"}::{name}>()')
text='// Generated from internal_raft.proto and internal_managed.proto.\nconst SCHEMAS:&[&[Field]]=&[\n'+'\n'.join(lines)+'\n];\n'
text+='const SIZES:&[usize]=&['+','.join(sizes)+'];\n'
for name in ['VoteRequest','VoteResponse','AppendRequest','AppendResponse','SnapshotRequest','SnapshotResponse']:
 text+=f'impl BoundedMessage for wire::{name} {{ fn schema()->usize{{{lookup[("r",name)]}}} }}\n'
(root/'crates/d2-raft/src/raft_grpc/scan_schema.rs').write_text(text)
