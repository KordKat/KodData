package hello1.koddata.sessions.users;

public record UserPrivilege(int maxSession ,
                            int maxProcessPerSession,
                            int maxMemoryPerProcess,
                            int maxStorageUsage) {

}


//เก็บ user ลง file program เริ่มโหลดขึ้น memory
//แบ่งข้อมูล user to arrybyte
//end to Terminate
//up in u
