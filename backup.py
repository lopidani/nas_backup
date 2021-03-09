# !/usr/bin/env python 
# coding: utf-8 

# we create user NAS folder after a rule 'pcname_username'
# and after every backup we rename user folder to 'pcname_username_year.monyh.day' 
import os,getpass,stat,paramiko,param,datetime,win32api,win32con
# incremental upload (pyftpsync)
from ftpsync.targets import FsTarget
from ftpsync.ftp_target import FtpTarget
from ftpsync.synchronizers import UploadSynchronizer

#-------------------------------------
# win32api/win32.con file attributes
#FILE_ATTRIBUTE_ARCHIVE              = 32
#FILE_ATTRIBUTE_ATOMIC_WRITE         = 512
#FILE_ATTRIBUTE_COMPRESSED           = 2048
#FILE_ATTRIBUTE_DEVICE               = 64
#FILE_ATTRIBUTE_DIRECTORY            = 16
#FILE_ATTRIBUTE_ENCRYPTED            = 16384
#FILE_ATTRIBUTE_HIDDEN               = 2
#FILE_ATTRIBUTE_NORMAL               = 128
#FILE_ATTRIBUTE_NOT_CONTENT_INDEXED  = 8192
#FILE_ATTRIBUTE_OFFLINE              = 4096
#FILE_ATTRIBUTE_READONLY             = 1
#FILE_ATTRIBUTE_REPARSE_POINT        = 1024
#FILE_ATTRIBUTE_SPARSE_FILE          = 512
#FILE_ATTRIBUTE_SYSTEM               = 4
#FILE_ATTRIBUTE_TEMPORARY            = 256
#FILE_ATTRIBUTE_VIRTUAL              = 65536
#FILE_ATTRIBUTE_XACTION_WRITE        = 1024
#--------------------------------------


# parameters
pc_name=os.environ['COMPUTERNAME']
pc_user=os.environ['USERNAME']
now=datetime.datetime.now().isoformat(" ")[0:10].replace('-','.')
# nas user until backup
nas_user_initial='_'.join((pc_name,pc_user))
# nas user after bacup
nas_user_final='_'.join((pc_name,pc_user,now))
# log file
lF='\\'.join(("C:\\Users",pc_user,"AppData\\Local\\Nas_backup\\backup_errors.txt"))
# upload frequency in days
fr=5

# LUCRU !!! de luat parametrii serverului din o locatie fixa (pe un alt server)


class SSHNas(object):
      def __init__(self,ip,port,user,passw):
          self.ip=ip
          self.port=port
          self.user=user
          self.passw=passw
          self.ssh = paramiko.SSHClient()
          self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
          self.ssh.connect(ip,port=port,username=user,password=passw)
          self.sftp = self.ssh.open_sftp()
          self.fold=None

      def ssh_command(self,command,file,error=None):
          # read - write permission
          if command=="read/write":
             stdin,stdout,stderr = self.ssh.exec_command(" ".join(("chmod 777",file)))   
          # create txt file and permission read , write
          elif command=="cat >":
             stdin,stdout,stderr = self.ssh.exec_command(" ".join((command,file)))
             stdin,stdout,stderr = self.ssh.exec_command(" ".join(("chmod 777",file))) 
          # read txt files on NAS (source_backup.txt)
          elif command=="cat":
             stdin,stdout,stderr = self.ssh.exec_command(" ".join((command,file)))
             # avoid unicode representation , remain only string ( if we use stdout.readlines()
             # we will have a unicode representation of list elements)
             result = stdout.read().splitlines() 
             return result     
          elif command=="df -h": 
               stdin,stdout,stderr = self.ssh.exec_command(" ".join((command,file)))
               result = stdout.readlines()   
               result="".join(result)
               l0=[]
               l1=(result.splitlines( ))[1].splitlines( )[0].split(" ")
               for s in l1:
                   if s!=u'':
                      l0.append(s)  
               free=str(l0[3])
               #print 'FREE SPACE NAS=',free
               # change from GB in Byte
               if 'G' in free:
                  free=free.replace('G','')
                  free=float(free)*1073741824
               elif 'M' in free:
                    free=free.replace('M','')
                    free=float(free)*1048576
               elif 'K' in free:
                    free=free.replace('K','')
                    free=float(free)*1024    
               return free
          #--------------------------------------------
          # no useful we use paramiko sfpt chown method     
          #elif command=="add owner":
          #     #stdin,stdout,stderr = self.ssh.exec_command(" ".join(("chown -R itc",file+"/*")))
          #     stdin,stdout,stderr = self.ssh.exec_command(" ".join(("chown -R itc:users",file)))
          #     #stdin,stdout,stderr = self.ssh.exec_command(" ".join(("chown itc:users",file)))
          #--------------------------------------------     
          elif command=="rm dir":
               stdin,stdout,stderr = self.ssh.exec_command(" ".join(("rm -rf",file)))
          # de lucrat de adaugat linii noi in text file
          elif command=="add line to txt file":
               #stdin,stdout,stderr = self.ssh.exec_command(" ".join(("echo "+error+" >>",file)))
               stdin,stdout,stderr = self.ssh.exec_command(" ".join(("echo -n "+error+" >>",file)))
          elif command=="clear txt file":
               stdin,stdout,stderr = self.ssh.exec_command(" ".join(("echo -n "" >",file)))
          # find users UID (our user on NAS is itc)
          elif command=="get user UID":
               stdin,stdout,stderr = self.ssh.exec_command(" ".join(("id -u itc",file)))
               result = stdout.readlines()
               return int(result[0])
          # find users GID (our user on NAS is itc)
          elif command=="get user GID":
               stdin,stdout,stderr = self.ssh.exec_command(" ".join(("id -g itc",file)))
               result = stdout.readlines()
               return int(result[0])         

      def check_network(self):
          response=os.system(" ".join(("ping -n 2",param.nas_ip)))                   
          if response==0:
             return True
          else: return None 

      # create user folder and backup_sources.txt on file server
      def create_user_folder(self,dest,name):
          if self.find_folder_name(dest,name) is None :
             self.sftp.mkdir('/'.join((dest,name)))
             self.sftp.chown('/'.join((dest,name)),self.ssh_command("get user UID",""),self.ssh_command("get user GID",""))  
             self.ssh_command("read/write",'/'.join((dest,name)))
          # create backup_sources.txt if not exist   
          try:
              # all name from NAS
              self.sftp.stat('/'.join((self.find_folder_name(dest,name),"backup_sources.txt")))
          except IOError:
                        # all name from NAS
                        self.ssh_command("cat >",'/'.join((self.find_folder_name(dest,name),"backup_sources.txt")))
          #-- creez errors de la inceput sau cand apare o eroare ???
          try:
              # all name from NAS
              self.sftp.stat('/'.join((self.find_folder_name(dest,name),"backup_errors.txt")))
          except IOError:
                        # all name from NAS
                        self.ssh_command("cat >",'/'.join((self.find_folder_name(dest,name),"backup_errors.txt")))                          
          #-------------------------- ???
       
      # destination - where to check
      # source      - what to find
      def find_folder_name(self,destination,source):
          fname=None
          for fname in self.sftp.listdir(destination):    
              if source in fname:
                 # all name from NAS
                 return '/'.join((destination,fname))
                 break
      
      # write errors first on PC
      def edit_errors(self,logStr,logFile):
          with open(logFile,'a+') as lf: 
                lf.write(logStr)
      
      # pc path C:\Applic97/New1\\proba.txt will be transformed 
      # in list ['C','Applic97','New1'] to make folders on server
      def path_to_list(self,path):
          s1=path.replace(':','')
          s2=s1.replace('/','|')
          s3=s2.replace('\\','|') 
          if os.path.isdir(path)==True:
             l=s3.split('|') 
             self.fold=[x for x in l if x not in [''," "*len(x)]] 
          elif os.path.isfile(path)==True:
               l=s3.split('|')[:-1]  
               self.fold=[x for x in l if x not in [''," "*len(x)]] 
          return self.fold 

      # dest == nas_dest
      def make_nas_folders(self,dest,f):
          folders=self.path_to_list(f)
          folders_paths=[] 
          if len(folders) !=0:    
             for i in range(len(folders)):
                 if i==0:
                    folders_paths.append(str('/'.join((dest,folders[i]))))
                 else: folders_paths.append(str('/'.join((folders_paths[i-1],folders[i]))))
          if len(folders_paths) != 0:  
             for path in folders_paths:
                 try: 
                     self.sftp.stat(path)   
                 except IOError:
                                try: 
                                    # create folders with read/write permission             
                                    self.sftp.mkdir(path) 
                                    #self.ssh.exec_command("cwd "+path)
                                    self.sftp.chdir(path)
                                    self.sftp.chown(path,self.ssh_command("get user UID",""),self.ssh_command("get user GID",""))  
                                    self.ssh_command("read/write",path)
                                except IOError:
                                               # incorrect write of paths on backup_sources.txt
                                               txt=".Folder has no permision on PC (folder system in use) or wrong written in NAS backup_sources.txt"
                                               error=' '.join(("ERR2 - Can't copy folder",f,txt))
                                               # write errors first on PC  
                                               self.edit_errors(error+'\n',lF)                                                     
                                                            
      # create files and folders on a destination from a writable
      # text file "backup_sources.txt"
      def create_files_folders(self,dest,txt_file_path):
          txt=self.sftp.open(txt_file_path,'r')
          # again representation of elements in string format
          string_files=txt.read().splitlines()
          full_user=self.find_folder_name('/backup',nas_user_initial) 
          for f in string_files:
              if os.path.exists(f)==True: 
                 if os.path.isdir(str(f))==True: 
                    # eliminate ":" from string and create folders
                    self.make_nas_folders(dest,f)      
                 # even if we have files we must create folders                  
                 elif os.path.isfile(f)==True:
                      dt_mod_pc = datetime.datetime.fromtimestamp(os.stat(f).st_mtime)#.strftime('%Y.%m.%d-%H:%M')
                      self.make_nas_folders(dest,f)                    
                      # we will upload file only if st_mtime differ from NAS and PC                     
                      nas_file=f.replace(':','').replace('\\','/')
                      path='/'.join((full_user,nas_file))
                      # if file exist take st_mtime
                      try:
                          self.sftp.stat(path) 
                          dt_mod_nas=datetime.datetime.fromtimestamp(self.sftp.stat(path).st_mtime)#.strftime('%Y.%m.%d-%H:%M')
                      except IOError:
                                    # if file not exist i will upload from PC
                                    try:
                                        self.sftp.put(f,path)
                                        dt_mod_nas=datetime.datetime.fromtimestamp(self.sftp.stat(path).st_mtime)#.strftime('%Y.%m.%d-%H:%M')
                                        self.sftp.chown(path,self.ssh_command("get user UID",""),self.ssh_command("get user GID",""))
                                    except IOError:
                                                    # error can't copy file because file have permission denied on PC
                                                    # (is a file system in use) or file to be copied was wrong in NAS
                                                    # in backup_souces.txt
                                                    txt=".File has no permision on PC (file system in use) or wrong written in NAS backup_sources.txt"
                                                    error=' '.join(("ERR2 - Can't copy file",f,txt))
                                                    # write errors first on PC   
                                                    self.edit_errors(error+'\n',lF) 
                                                    dt_mod_nas=None   
                      dt_mod_pc = datetime.datetime.fromtimestamp(os.stat(f).st_mtime)#.strftime('%Y.%m.%d-%H:%M')   
                      if (dt_mod_nas)  and (dt_mod_nas <= dt_mod_pc) : # ptr probe >=   
                         # make upload
                         try:
                             self.sftp.put(f,path)
                             self.sftp.chown(path,self.ssh_command("get user UID",""),self.ssh_command("get user GID","")) 
                         except IOError:
                                        error=' '.join(('ERROR2 ==> incorect write for Windows',f,'.Edit',txt_file_path,'with right path !'))
                                        # write errors first on PC   
                                        self.edit_errors(error+'\n',lF)
              elif f in [''," "*len(f)]:
                   continue 
              else:
                   error=' '.join(("ERR : Path-ul ==>",f,"<== nu exista , sau este incorect scris editeaza backup_sources.txt"))
                   self.edit_errors(error+'\n',lF)

      # datetime string format to datetime date format
      def string2datetime(self,string):
          try:
              dt=datetime.datetime.strptime(string,"%Y.%m.%d")
              return dt
          except ValueError: return None 

      # if local log file in not empty upload to server
      def check_local_log(self,pth,error=None):
          if os.path.isfile(pth)==True:
             try:
                 self.sftp.put(pth,'/'.join(('/backup',nas_user_final,"backup_errors.txt"))) 
             except Exception as e:
                    self.edit_errors(str(e)+'\n',lF)

      #-------------------------------------------------------
      # check file system - only if file/directory is not system 
      # file/directory we can go further and make upload 
      def check_system_file(self,path):
          attr=win32api.GetFileAttributes(path)
          if attr & (win32con.FILE_ATTRIBUTE_SYSTEM) != 4: 
             return True
      #-------------------------------------------------------         

                     
# class to upload (incremental) a folder from PC to a folder 
# on an server file server with SSH and FTP enabled            
class UPload(object):
      def __init__(self):
          self.settings = {"force": False, "delete_unmatched": False,"verbose": 3}
          #self.settings = {"force": False, "delete_unmatched": False,"ftp_active":True,"verbose": 3}

      # upload folder from PC to FTP server(NAS)
      # source must be a folder even if we have to backup a file
      # we will take folder were file is
      def upload(self,source,dest,host,user,passwd): 
          source=FsTarget(source)
          dest=FtpTarget(dest,host,username=user,password=passwd)
          job=UploadSynchronizer(source,dest,self.settings)
          job.run()   


SSH=SSHNas(param.nas_ip,param.ssh_port,param.ssh_user,param.ssh_passw)
if SSH.check_network():
   # prima data creez folderul 'pcname_username_an.luna.zi'
   SSH.create_user_folder('/backup',nas_user_initial)
   full_user=SSH.find_folder_name('/backup',nas_user_initial)
   bkp_txt_name=SSH.find_folder_name(full_user,"backup_sources.txt")          
   if len(SSH.ssh_command("cat",bkp_txt_name)) != 0:
      # the size of all files that we want to upload
      source_size=[os.path.getsize(f) for f in SSH.ssh_command("cat",bkp_txt_name) if os.path.exists(f) == True ]
      NAS_size=SSH.ssh_command("df -h","/c/backup") 
      # if we have space on NAS we make backup            
      if NAS_size > sum(source_size):   
         if full_user: 
            dt_str=str(full_user).split('_')[-1]                
            #if (SSH.string2datetime(dt_str) is None)  or (SSH.string2datetime(dt_str)+datetime.timedelta(days=param.fr)>= datetime.datetime.now()): #<=
            if (SSH.string2datetime(dt_str) is None)  or (SSH.string2datetime(dt_str)+datetime.timedelta(days=fr)<= datetime.datetime.now()): # ptr teste schimba >=        
                  SSH.create_files_folders(full_user,bkp_txt_name)                        
                  for f in SSH.ssh_command("cat",bkp_txt_name):
                      if os.path.exists(f)==True:      
                         # check if folders are on PC
                         if os.path.isdir(f) == True :  
                            s='/'.join([x for x in f.replace(':','').split('\\') if x not in ['',' '*len(x)]])
                            dest=str('/'.join((full_user,s)))
                            try:
                                UPload().upload(f,dest,param.nas_ip,param.ftp_user,param.ftp_passw)
                            except Exception as e:
                                                  SSH.edit_errors(str(e)+'\n',lF)                           
                  # after every upload we rename NAS folder 'pcname_username_year.month.day'
                  try:      
                        SSH.sftp.rename(full_user,'/backup/'+nas_user_final)
                  except IOError:
                                 pass          
      else:
           error="NU POT FACE BACKUP , NU AM SPATIU PE SERVER !"      
           SSH.edit_errors(error+'\n',lF)                    
      # upload log file on server                    
      SSH.check_local_log(lF)  
      # clear local log file
      if os.path.exists(lF)==True:
         with open(lF,'w') as lf:
              pass      
      SSH.sftp.close()                 
      SSH.ssh.close()  