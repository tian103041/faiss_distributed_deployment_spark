package spark_redis_pubsub;

import redis.clients.jedis.JedisPubSub;

public class Subscriber extends JedisPubSub {
    Subscriber() {
    }

    public void onMessage(String channel, String message) {
        String message_need = "the faiss distributed data is ready!";
        String channel_need = "faiss_spark_to_master_data_pub";

        if (message.equals(message_need)  && channel.equals(channel_need)) {

            System.out.println(String.format("receive redis published message, channel %s, message %s", channel, message));
            //启动运行spark程序的shell脚本
            String path_command = "/home/11085098/faiss_spark_auto.sh";
            call_shell call_shell_exe = new call_shell();
            call_shell_exe.call_shell_method(path_command);
        }
    }

    public void onSubscribe(String channel, int subscribedChannels) {
        System.out.println(String.format("subscribe redis channel success, channel %s, subscribedChannels %d",
                channel, subscribedChannels));
    }

    public void onUnsubscribe(String channel, int subscribedChannels) {
        System.out.println(String.format("unsubscribe redis channel, channel %s, subscribedChannels %d",
                channel, subscribedChannels));

    }
}
