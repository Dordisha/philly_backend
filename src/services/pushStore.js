import { readFileSync, writeFileSync, renameSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, isAbsolute } from 'node:path';
import { createHash, randomBytes, createCipheriv, createDecipheriv } from 'node:crypto';
export const digest = value => createHash('sha256').update(value).digest('hex');
// Single server process, mounted private persistent disk. No tokens in logs or responses.
export class PushStore {
  constructor(file, key) {
    if (!isAbsolute(file || '') || !/^[a-f0-9]{64}$/i.test(key || '')) throw new Error('Push storage is not configured');
    this.file=file; this.key=Buffer.from(key,'hex');
    mkdirSync(dirname(file), {recursive:true,mode:0o700});
    this.data=existsSync(file) ? JSON.parse(readFileSync(file,'utf8')) : {devices:{},campaigns:{}};
    if (!this.data.devices || !this.data.campaigns) throw new Error('Invalid push store');
  }
  save() {
    const temp=this.file+'.tmp';
    writeFileSync(temp,JSON.stringify(this.data),{mode:0o600}); renameSync(temp,this.file);
  }
  encrypt(token) {
    const iv=randomBytes(12), cipher=createCipheriv('aes-256-gcm',this.key,iv);
    const value=Buffer.concat([cipher.update(token,'utf8'),cipher.final()]);
    return [iv,cipher.getAuthTag(),value].map(b=>b.toString('base64')).join('.');
  }
  decrypt(value) {
    const [iv,tag,data]=value.split('.').map(x=>Buffer.from(x,'base64'));
    const cipher=createDecipheriv('aes-256-gcm',this.key,iv);cipher.setAuthTag(tag);
    return Buffer.concat([cipher.update(data),cipher.final()]).toString();
  }
}
