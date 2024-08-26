/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.email.sink;

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.email.config.EmailSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.email.exception.EmailConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.email.exception.EmailConnectorException;

import com.sun.mail.util.MailSSLSocketFactory;
import lombok.extern.slf4j.Slf4j;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class EmailSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private final SeaTunnelRowType seaTunnelRowType;
    private final EmailSinkConfig config;
    private StringBuffer stringBuffer;

    public EmailSinkWriter(SeaTunnelRowType seaTunnelRowType, EmailSinkConfig pluginConfig) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.config = pluginConfig;
        this.stringBuffer = new StringBuffer();
    }

    @Override
    public void write(SeaTunnelRow element) {
        Object[] fields = element.getFields();

        for (Object field : fields) {
            stringBuffer.append(field.toString() + ",");
        }
        stringBuffer.deleteCharAt(fields.length - 1);
        stringBuffer.append("\n");
    }

    @Override
    public void close() {
        createFile();
        Properties properties = new Properties();
        properties.setProperty("mail.host", config.getEmailHost());
        properties.setProperty("mail.transport.protocol", config.getEmailTransportProtocol());
        properties.setProperty("mail.smtp.auth", config.getEmailSmtpAuth().toString());
        properties.setProperty("mail.smtp.port", config.getEmailSmtpPort().toString());

        try {
            MailSSLSocketFactory sf = new MailSSLSocketFactory();
            sf.setTrustAllHosts(true);
            properties.put("mail.smtp.ssl.socketFactory", sf);
            Session session;
            if (config.getEmailSmtpAuth()) {
                properties.put("mail.smtp.ssl.enable", "true");
                session =
                        Session.getDefaultInstance(
                                properties,
                                new Authenticator() {
                                    @Override
                                    protected PasswordAuthentication getPasswordAuthentication() {
                                        return new PasswordAuthentication(
                                                config.getEmailFromAddress(),
                                                config.getEmailAuthorizationCode());
                                    }
                                });
            } else {
                session = Session.getDefaultInstance(properties);
            }
            // Create the default MimeMessage object
            MimeMessage message = new MimeMessage(session);

            // Set the email address
            message.setFrom(new InternetAddress(config.getEmailFromAddress()));

            // Set the recipient email address
            String[] emailAddresses = config.getEmailToAddress().split(",");
            Address[] addresses = new Address[emailAddresses.length];
            for (int i = 0; i < emailAddresses.length; i++) {
                addresses[i] = new InternetAddress(emailAddresses[i]);
            }
            if (addresses.length > 0) {
                message.setRecipients(Message.RecipientType.TO, addresses);
            }

            // Setting the Email subject
            message.setSubject(config.getEmailMessageHeadline());

            // Create Message
            BodyPart messageBodyPart = new MimeBodyPart();

            // Set Message content
            messageBodyPart.setText(config.getEmailMessageContent());

            // Create multiple messages
            Multipart multipart = new MimeMultipart();
            // Set up the text message section
            multipart.addBodyPart(messageBodyPart);
            // accessory
            messageBodyPart = new MimeBodyPart();
            String filename = "emailsink.csv";
            DataSource source = new FileDataSource(filename);
            messageBodyPart.setDataHandler(new DataHandler(source));
            messageBodyPart.setFileName(filename);
            multipart.addBodyPart(messageBodyPart);
            message.setContent(multipart);

            //   send a message
            Transport.send(message);
            log.info("Sent message successfully....");
        } catch (Exception e) {
            throw new EmailConnectorException(
                    EmailConnectorErrorCode.SEND_EMAIL_FAILED, "Send email failed", e);
        }
    }

    public void createFile() {
        String fileName = "emailsink.csv";
        try {
            String data = stringBuffer.toString();
            File file = new File(fileName);
            // if file doesn't exist, then create it
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file.getName());
            fileWriter.write(data);
            fileWriter.close();
            log.info("Create File successfully....");
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("Email", "create", fileName, e);
        }
    }
}
