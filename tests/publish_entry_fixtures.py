"""Commit one complete generation of genuine declared test artifacts.

An exclusive publisher lock refuses concurrent/foreign mutation. The final
marker records complete actual image bytes; the consumer captures that exact
unit before the original assertions. These are mutable derived outputs only.
"""
from pathlib import Path
import os, stat, json, hashlib, ctypes
SCHEMA = 'codetracer-trace-format.entry-fixtures.v1'
NAMES = {'marked.ct', 'unmarked.ct', '.producer-owner'}

def unique_keys(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise RuntimeError('Duplicate generation marker key')
        result[key]=value
    return result

def identity(path):
    path = Path(path); info = path.lstat()
    if path.is_symlink() or getattr(info, 'st_file_attributes', 0) & 0x400:
        raise RuntimeError('Reparse/link authority refused')
    return (info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode), stat.S_IMODE(info.st_mode), str(path.resolve(strict=True)))

def held_file(path):
    path = Path(path); before = identity(path)
    if before[2] != stat.S_IFREG: raise RuntimeError('Nonordinary artifact')
    if os.name == 'nt':
        import msvcrt
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.CreateFileW.argtypes = [ctypes.c_wchar_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p]
        kernel.CreateFileW.restype = ctypes.c_void_p
        handle = kernel.CreateFileW(str(path),0x80000000,1,None,3,0x00200000,None)
        if handle in (None,ctypes.c_void_p(-1).value): raise OSError(ctypes.get_last_error(),'Cannot hold artifact')
        fd = msvcrt.open_osfhandle(handle,os.O_RDONLY|os.O_BINARY)
    else:
        fd = os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    file = os.fdopen(fd,'rb')
    actual = os.fstat(file.fileno())
    if (actual.st_dev,actual.st_ino,stat.S_IFMT(actual.st_mode),stat.S_IMODE(actual.st_mode)) != before[:4]:
        file.close(); raise RuntimeError('Artifact image changed on admission')
    return file,before

class HeldDirectory:
    def __init__(self,path):
        self.path=Path(path);self.before=identity(self.path)
        if self.before[2]!=stat.S_IFDIR: raise RuntimeError('Nonordinary directory')
        if os.name=='nt':
            self.kernel=ctypes.WinDLL('kernel32',use_last_error=True)
            self.kernel.CreateFileW.argtypes=[ctypes.c_wchar_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p]
            self.kernel.CreateFileW.restype=ctypes.c_void_p
            self.kernel.CloseHandle.argtypes=[ctypes.c_void_p]
            # Read attributes; share read/write but NOT deletion of this parent.
            self.handle=self.kernel.CreateFileW(str(self.path),0x80,3,None,3,0x02200000,None)
            if self.handle in (None,ctypes.c_void_p(-1).value): raise OSError(ctypes.get_last_error(),'Cannot hold output parent')
            class Info(ctypes.Structure):
                _pack_=4
                _fields_=[('attributes',ctypes.c_uint32),('creation',ctypes.c_uint64),('access',ctypes.c_uint64),('write',ctypes.c_uint64),('volume',ctypes.c_uint32),('sizeHigh',ctypes.c_uint32),('sizeLow',ctypes.c_uint32),('links',ctypes.c_uint32),('indexHigh',ctypes.c_uint32),('indexLow',ctypes.c_uint32)]
            self.Info=Info
            self.kernel.GetFileInformationByHandle.argtypes=[ctypes.c_void_p,ctypes.POINTER(Info)]
            self.kernel.GetFileInformationByHandle.restype=ctypes.c_int
            self.native=self.handle_identity()
            # Bind held handle to admitted physical volume/file index.
            if self.native[0]!=self.before[0] or ((self.native[1]<<32)|self.native[2])!=self.before[1]:
                self.close();raise RuntimeError('Changed held Windows parent')
        else:
            self.fd=os.open(self.path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            actual=os.fstat(self.fd)
            if (actual.st_dev,actual.st_ino,stat.S_IFMT(actual.st_mode),stat.S_IMODE(actual.st_mode))!=self.before[:4]:
                self.close();raise RuntimeError('Changed held POSIX parent')
        self.check()
    def handle_identity(self):
        info=self.Info()
        if not self.kernel.GetFileInformationByHandle(self.handle,ctypes.byref(info)):raise OSError(ctypes.get_last_error(),'Held parent query failed')
        if info.attributes&0x400 or not info.attributes&0x10:raise RuntimeError('Foreign held native parent')
        return (info.volume,info.indexHigh,info.indexLow,info.attributes)
    def check(self):
        if identity(self.path)!=self.before:raise RuntimeError('Changed lexical/resolved parent authority')
        if os.name=='nt':
            if self.handle_identity()!=self.native:raise RuntimeError('Changed held native directory')
        else:
            info=os.fstat(self.fd)
            if (info.st_dev,info.st_ino,stat.S_IFMT(info.st_mode),stat.S_IMODE(info.st_mode))!=self.before[:4]:raise RuntimeError('Changed held directory')
    def close(self):
        if os.name=='nt':self.kernel.CloseHandle(self.handle)
        else:os.close(self.fd)

def publish(root,source,destination):
    root=Path(root).resolve(strict=True);destination=Path(destination).absolute()
    if destination!=root/'target'/'entry-fixtures':raise RuntimeError('Foreign output namespace')
    parents=[];images=[];lock=None
    try:
        for parent in [root,root/'target']:parents.append(HeldDirectory(parent))
        if os.name=='nt':destination.mkdir(mode=0o700,exist_ok=True)
        else:
            try:os.mkdir('entry-fixtures',mode=0o700,dir_fd=parents[-1].fd)
            except FileExistsError:pass
        parents.append(HeldDirectory(destination))
        destination_fd=None if os.name=='nt' else parents[-1].fd
        lock=destination/'.publisher-lock'
        lock_fd=os.open(lock if os.name=='nt' else '.publisher-lock',os.O_WRONLY|os.O_CREAT|os.O_EXCL| (0 if os.name=='nt' else os.O_NOFOLLOW),0o600,**({} if os.name=='nt' else {'dir_fd':destination_fd}))
        with os.fdopen(lock_fd,'wb') as stream:stream.write(SCHEMA.encode()+b'\n');stream.flush();os.fsync(stream.fileno())
        lock_identity=identity(lock)
        members=set(os.listdir(destination if os.name=='nt' else destination_fd))-{'.publisher-lock'}
        if members and members!=NAMES:raise RuntimeError('Incomplete/foreign previous output unit')
        if members:
            old={}
            for name in NAMES:
                file,before=held_file(destination/name);body=file.read();file.close();old[name]=body
                if identity(destination/name)!=before:raise RuntimeError('Changed previous output unit')
            marker=json.loads(old['.producer-owner'],object_pairs_hook=unique_keys)
            if set(marker)!={'schema','images'} or marker.get('schema')!=SCHEMA or set(marker.get('images',{}))!={'marked.ct','unmarked.ct'}:raise RuntimeError('Foreign generation marker')
            for name in ['marked.ct','unmarked.ct']:
                if marker['images'][name]!=old[name].hex():raise RuntimeError('Previous generation correspondence refused')
        payloads={}
        for name in ['marked.ct','unmarked.ct']:
            file,before=held_file(Path(source)/name);body=file.read();file.seek(0)
            if file.read()!=body or identity(Path(source)/name)!=before:raise RuntimeError('Changed producer artifact')
            images.append((file,Path(source)/name,before,body));payloads[name]=body
        payloads['.producer-owner']=(json.dumps({'schema':SCHEMA,'images':{name:payloads[name].hex() for name in ['marked.ct','unmarked.ct']}},sort_keys=True)+'\n').encode()
        for name,body in payloads.items():
            for parent in parents:parent.check()
            if identity(lock)!=lock_identity:raise RuntimeError('Changed exclusive publisher lock')
            temporary=destination/('.entry-output-'+os.urandom(12).hex())
            fd=os.open(temporary if os.name=='nt' else temporary.name,os.O_WRONLY|os.O_CREAT|os.O_EXCL| (0 if os.name=='nt' else os.O_NOFOLLOW),0o600,**({} if os.name=='nt' else {'dir_fd':destination_fd}))
            with os.fdopen(fd,'wb') as stream:stream.write(body);stream.flush();os.fsync(stream.fileno())
            for parent in parents:parent.check()
            if os.name=='nt':os.replace(temporary,destination/name)
            else:os.replace(temporary.name,name,src_dir_fd=destination_fd,dst_dir_fd=destination_fd)
        if set(os.listdir(destination if os.name=='nt' else destination_fd))!=NAMES|{'.publisher-lock'}:raise RuntimeError('Changed final output membership')
        for name,body in payloads.items():
            file,before=held_file(destination/name)
            with file:
                if file.read()!=body or identity(destination/name)!=before:raise RuntimeError('Changed committed output bytes')
        for file,path,before,body in images:
            file.seek(0)
            if file.read()!=body or identity(path)!=before:raise RuntimeError('Producer changed during publication')
        for parent in parents:parent.check()
        if identity(lock)!=lock_identity:raise RuntimeError('Foreign publisher lock on release')
        if os.name=='nt':lock.unlink()
        else:os.unlink('.publisher-lock',dir_fd=destination_fd)
        lock=None
    finally:
        for file,*_ in images:file.close()
        for parent in reversed(parents):parent.close()
        # Failure keeps the lock/partial output evidence; no unknown cleanup.
