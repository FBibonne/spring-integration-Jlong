package fr.insee.test;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ImageBanner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.ftp.dsl.Ftp;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ReflectionUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class App
{
    private String ascii="ascii";

    public static void main(String[] args )
    {
    }

    @Bean
    Exchange exchange(){
        return ExchangeBuilder.directExchange(this.ascii).durable(true).build();
    }

    @Bean
    Queue queue(){
        return QueueBuilder.durable(this.ascii).build();
    }

    @Bean
    Binding binding(){
        return BindingBuilder.bind(queue()).to(exchange()).with(this.ascii).noargs();
    }

    @Configuration
    public static class FtpConfig {


        @Bean
        DefaultFtpSessionFactory ftpSessionFactory(@Value("${ftp.port:2121}") int port,
                                                   @Value("${ftp.user:jlong}") String user,
                                                   @Value("${ftp.password:spring}") String password) {
            DefaultFtpSessionFactory factory = new DefaultFtpSessionFactory();
            factory.setPort(port);
            factory.setUsername(user);
            factory.setPassword(password);
            return factory;
        }
    }

    @Bean
    IntegrationFlow files(@Value("${input-dir:${HOME}/Desktop/in}") String in, Environment env) {
        GenericTransformer<File, Message<String>> fileStringGenericTransformer=source->{
            try(ByteArrayOutputStream baos=new ByteArrayOutputStream();
                PrintStream printStream=new PrintStream(baos)){
                var imageBanner=new ImageBanner(new FileSystemResource(source));
                imageBanner.printBanner(env,getClass(),printStream);
                return MessageBuilder.withPayload(baos.toString())
                        .setHeader(FileHeaders.FILENAME,source.getAbsoluteFile().getName())
                        .build();
            }catch (IOException e){
                ReflectionUtils.rethrowRuntimeException(e);
            }
            return null;
        };
        return IntegrationFlows
                .from(Files.inboundAdapter(new File(in))
                        .autoCreateDirectory(true).preventDuplicates(true).patternFilter("*.jpg"),
                        poller -> poller.poller(pm -> pm.fixedRate(1000)))
                .transform(File.class, fileStringGenericTransformer)
                .channel(this.asciiProcessors())
        .get();
    }

    @Bean
    IntegrationFlow amqp(AmqpTemplate amqpTemplate) {
        return IntegrationFlows
                .from(this.asciiProcessors())
                .handle(Amqp.outboundAdapter(amqpTemplate).exchangeName(this.ascii).routingKey(this.ascii))
                .get();
    }

    @Bean
    IntegrationFlow ftp(DefaultFtpSessionFactory ftpSessionFactory){
        return IntegrationFlows
                .from(this.asciiProcessors())
                .handle(Ftp.outboundAdapter(ftpSessionFactory)
                        .remoteDirectory("uploads")
                        .fileNameGenerator(message -> ((String) message.getHeaders().get(FileHeaders.FILENAME)).split("\\.")[0] + ".txt")
                )
                .get();
    }

    @Bean
    MessageChannel asciiProcessors(){
        return MessageChannels.publishSubscribe().get();
    }



}
