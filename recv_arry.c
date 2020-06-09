#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <librdkafka/rdkafka.h>

#define	SER_PORT	12345
#define IPADDR		"224.1.2.3"
#define LOCAL_IPADDR    "192.168.205.128"
#define BUF_SIZE        512

//分配1M
static char *buf_kafka[1024*1024*1] = {};

static volatile sig_atomic_t run = 1;
static volatile sig_atomic_t flag = 1;

/**
 * @brief Signal termination of program
 */
static void stop (int sig) {
    run = 0;
    flag = 0;
}

static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *opaque) {
    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n",
                rd_kafka_err2str(rkmessage->err));
    //else
    //    fprintf(stderr,
    //            "%% Message delivered (%zd bytes, "
    //            "partition %"PRId32")\n",
    //            rkmessage->len, rkmessage->partition);
    /* The rkmessage is destroyed automatically by librdkafka */
}

void *thread_kafka(void *arg)
{
    pthread_t  tid_kafka;    
    int k=0,ret;
    rd_kafka_t *rk;         /* Producer instance handle */
    rd_kafka_conf_t *conf;  /* Temporary configuration object */
    char errstr[512];       /* librdkafka API error reporting buffer */
    const char *brokers;    /* Argument: broker list */
    const char *topic;      /* Argument: topic to produce to */
    char *buf;
    
    brokers = "192.168.205.128";
    topic   = "test_topic";
    
    conf = rd_kafka_conf_new();//创建rd_kafka_conf_t对象conf
    
    //配置brokers
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
       fprintf(stderr, "%s\n", errstr);
       exit(1);
    }
    //设置回调函数dr_msg_cb
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
    //创建一个新的Kafka句柄,即创建producer实例
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
            fprintf(stderr,
                    "%% Failed to create new producer: %s\n", errstr);
            exit(1);
    }
    
    /* Signal handler for clean shutdown */
    signal(SIGINT, stop);
    
    //while (run && fgets(buf, sizeof(buf), stdin)) {
    while (run) {
    //if (run) {
        //strcpy(buf, "just test!");
	//printf("%s\n",buf);
        if(buf_kafka[k] != '\0' && strlen(buf_kafka[k]) != 0)
        {
            printf("%d:%s:%p\n",k,buf_kafka[k],buf_kafka[k]);
            //free(buf_kafka[k]);
        size_t len = strlen(buf_kafka[k]);
        rd_kafka_resp_err_t err;
    
        if (buf_kafka[k][len-1] == '\n') /* Remove newline */
                buf_kafka[k][--len] = '\0';
    
        if (len == 0) {
                /* Empty line: only serve delivery reports */
                rd_kafka_poll(rk, 0/*non-blocking */);//
		continue;
        }
    
    retry:
        err = rd_kafka_producev(//异步调用将消息发送到指定的topic
                    /* Producer handle */
                    rk,
                    /* Topic name */
                    RD_KAFKA_V_TOPIC(topic),
                    /* Make a copy of the payload. */
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    /* Message value and length */
                    RD_KAFKA_V_VALUE(buf_kafka[k], len),
                    /* Per-Message opaque, provided in
                     * delivery report callback as
                     * msg_opaque. */
                    RD_KAFKA_V_OPAQUE(NULL),
                    /* End sentinel */
                    RD_KAFKA_V_END);
    
        if (err) {
                    /*
                    * Failed to *enqueue* message for producing.
                    */
                    fprintf(stderr,
                            "%% Failed to produce to topic %s: %s\n",
                            topic, rd_kafka_err2str(err));
    
                    if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                            rd_kafka_poll(rk, 1000/*block for max 1000ms*/);
                            goto retry;
                    }
        } 
	//else {
        //       fprintf(stderr, "%% Enqueued message (%zd bytes) "
        //               "for topic %s\n",
        //               len, topic);
        //}
    
        rd_kafka_poll(rk, 0/*non-blocking*/);
	free(buf_kafka[k]);
        k++;
        }
        usleep(1);
    }
    
    
    //fprintf(stderr, "%% Flushing final messages..\n");
    rd_kafka_flush(rk, 10*1000 /* wait for max 10 seconds */);
    
    /* If the output queue is still not empty there is an issue
     * with producing messages to the clusters. */
    if (rd_kafka_outq_len(rk) > 0)
        fprintf(stderr, "%% %d message(s) were not delivered\n",
                rd_kafka_outq_len(rk));
    
    /* 销毁producer实例 */
    rd_kafka_destroy(rk);
    
    return 0;
}

//设置非阻塞
static void setnonblocking(int sockfd) {
    int flag = fcntl(sockfd, F_GETFL, 0);  //取标志
    if (flag < 0) {
        perror("fcntl F_GETFL fail");
        return;
    }
    if (fcntl(sockfd, F_SETFL, flag|O_NONBLOCK ) < 0) {  //设置标志
        perror("fcntl F_SETFL fail");
    }
}

int main(int argc, char **argv)
{
    int fd;
    int ret;
    char buf[BUF_SIZE] = {};
    //char *buf_kafka[10240] = {};
    struct sockaddr_in rec, send;
    socklen_t len;
    
    pthread_t  tid;    
                       
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if(-1 == fd)       
    {                  
    	perror("socket");
    	return -1;
    }                  
                       
    //设置UDP为非阻塞
    setnonblocking(fd);
                       
    rec.sin_family = AF_INET;
    rec.sin_port = htons(SER_PORT);
    rec.sin_addr.s_addr = INADDR_ANY;//inet_addr("0.0.0.0");
    len = sizeof(rec);
    
    ret = bind(fd, (struct sockaddr *)&rec, len);
    if(-1 == ret)
    {
    	perror("bind");
    	return -1;
    }
    
    struct ip_mreqn mreq;
    inet_aton(IPADDR, &mreq.imr_multiaddr);
    //inet_aton("192.168.110.250", &mreq.imr_address);
    inet_aton(LOCAL_IPADDR, &mreq.imr_address);
    mreq.imr_ifindex = 0;
    
    //把IP加入多播组224.1.2.3；
    ret = setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
    if(-1 == ret)
    {
    	perror("setsockopt");
    	return -1;
    }
    //开启子线程对kafka操作    
    if(pthread_create(&tid,NULL,thread_kafka,NULL)!=0)
    {
        printf("create pthread error:%s\n",strerror(ret));
    }

    len = sizeof(send);
    int i=0;
    while(flag)
    {
	//UDP消息收集
        ret = recvfrom(fd, buf, BUF_SIZE, 0, (struct sockaddr *)&send, &len);
    	if(-1 == ret)
    	{
    		//perror("recvfrom");
    		//return -1;
    		usleep(1);
    	}
    
	//数据保存起来
        if(buf != NULL && strlen(buf)!=0)
        {
            buf_kafka[i]=(char *)malloc(BUF_SIZE);
            strcpy(buf_kafka[i],buf);
	    i++;
            memset(buf,0,sizeof(buf));
            //printf("from[%s] port[%d] : %s\n", inet_ntoa(send.sin_addr), ntohs(send.sin_port), buf);
        }
    }
    close(fd);
    return 0;
}
