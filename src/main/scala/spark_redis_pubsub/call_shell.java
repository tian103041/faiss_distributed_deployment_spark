package spark_redis_pubsub;

class call_shell {

     void call_shell_method(String path_command){
        try {
            Process ps = Runtime.getRuntime().exec(path_command);
            ps.waitFor();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
