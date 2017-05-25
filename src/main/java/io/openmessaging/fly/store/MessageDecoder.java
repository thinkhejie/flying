package io.openmessaging.fly.store;

import java.nio.BufferUnderflowException;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultBytesMessage;

public class MessageDecoder {
	
	private final static Logger LOG = Logger.getLogger(MessageDecoder.class.getName());
	
	/** 编码格式 */
    public final static Charset       CHARSET_UTF8                 = Charset.forName("UTF-8");
	
    /**
     * 反序列化消息
     * 
     * @param byteBuffer
     * @return
     */
    public static BytesMessage decode(java.nio.ByteBuffer byteBuffer) {
        return decode(byteBuffer, true, true, false);
    }
    
	/**
     * 反序列化消息
     *
     * @param byteBuffer
     * @param readBody
     * @param deCompressBody
     * @param isRestoreNeeded
     * @return
     */
    public static BytesMessage decode(java.nio.ByteBuffer byteBuffer,  boolean readBody, boolean deCompressBody,  boolean isRestoreNeeded) {
        try {
        	byte[] body = null;
        	BytesMessage msgExt = new DefaultBytesMessage(body);
        	
            // 1 TOTALSIZE
            byteBuffer.getInt();

            // 2 MAGICCODE
            byteBuffer.getInt();

            // 3 BODYCRC
            byteBuffer.getInt();
            //msgExt.setBodyCRC(bodyCRC);

            // 4 QUEUEID
            byteBuffer.getInt();
            //msgExt.setQueueId(String.valueOf(queueId));

            // 5 QUEUEOFFSET
            byteBuffer.getLong();
            //msgExt.setOffset(queueOffset);

            // 6 PHYSICALOFFSET
            byteBuffer.getLong();

            // 7 SYSFLAG
            //int sysFlag = byteBuffer.getInt();
            //msgExt.setSysFlag(sysFlag);

            // 8 STORETIMESTAMP
            //long storeTimestamp = byteBuffer.getLong();
            //msgExt.setCreateTime(storeTimestamp);

            // 9 STOREHOST
            byte[] storeHost = new byte[4];
            byteBuffer.get(storeHost, 0, 4);
            int port = byteBuffer.getInt();
            //InetSocketAddress storeHostAddr = new InetSocketAddress(InetAddress.getByAddress(storeHost), port);
            //msgExt.setServerTag(storeHostAddr.toString());
            
            // 10 RECONSUMETIMES
            //int reconsumeTimes = byteBuffer.getInt();
            //msgExt.setReconsumeTimes(reconsumeTimes);

            // 11 TOPIC
            byte topicLen = byteBuffer.get();
            byte[] topic = new byte[(int) topicLen];
            byteBuffer.get(topic);
            msgExt.putHeaders(MessageHeader.TOPIC, new String(topic, CHARSET_UTF8));

            // 12 properties
            //short propertiesLength = byteBuffer.getShort();
            //if (propertiesLength > 0) {
                //byte[] properties = new byte[propertiesLength];
                //byteBuffer.get(properties);
                //String propertiesString = new String(properties, CHARSET_UTF8);
                //msgExt.setUserDefinedPropertiesString(propertiesString);
            //}

            // 13 context
           // short contextLength = byteBuffer.getShort();
            //if (contextLength > 0) {
                //byte[] context = new byte[contextLength];
                //byteBuffer.get(context);
                //String contextString = new String(context, CHARSET_UTF8);
                //Map<String, String> map = StringUtils.string2Map(contextString);
                //msgExt.setContextData(map);
            //}

            // 14 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                	body = new byte[bodyLen];
                    byteBuffer.get(body);
                    msgExt.setBody(body);
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 消息ID
            //ByteBuffer byteBufferMsgId = ByteBuffer.allocate(MSG_ID_LENGTH);
            //String msgId = createMessageId(byteBufferMsgId, UtilAll.SocketAddress2ByteBuffer(storeHostAddr).array(), commitLogOffset);
            //msgExt.setMsgId(msgId);

            if (isRestoreNeeded) {
                byteBuffer.position(0);
            }

            return msgExt;
        } catch (BufferUnderflowException e) {
            byteBuffer.position(byteBuffer.limit());
            LOG.severe("decode msg error:" + e);
        } catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
            LOG.severe("decode msg error:" + e);
        }
        return null;
    }
}
