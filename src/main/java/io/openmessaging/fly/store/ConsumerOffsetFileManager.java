package io.openmessaging.fly.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class ConsumerOffsetFileManager {
	
	private final static Logger LOG = Logger.getLogger(ConsumerOffsetFileManager.class.getName());
	
	private static String rootDir;
	
	/** TOPIC和GROUPID分隔符 */
	private static final String TOPIC_GROUP_SEPARATOR = "@";

	/**
	 * 获取消费进度路径
	 * 
	 * @param rootDir
	 * @return
	 */
	public static String getConsumerOffsetPath() {
		return rootDir + File.separator + "config" + File.separator + "consumerOffset.json";
	}

	/** 消费进度存储 KEY1:topic@group KEY2:queueId */
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> offsetTable = new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>(512);
	
	public ConsumerOffsetFileManager(String rootDir) {
		ConsumerOffsetFileManager.rootDir = rootDir;
	}

	/**
     * 提交消费进度
     * 
     * @param group
     * @param topic
     * @param queueId
     * @param offset
     */
    public void commitOffset(String topic, String queueId, long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR;
        ConcurrentHashMap<String, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<String, Long>(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            map.put(queueId, offset);
        }
    }
	
    public void decode(String jsonString) {
    	
    }
    
    public String encode(boolean prettyFormat) {
    	return "";
    }
    
    /**
	 * 加载配置
	 * 
	 * @return 加载结果
	 */
	public synchronized boolean load() {
		try {
			//从本地文件加载配置
			String fileName = getConsumerOffsetPath();
			String jsonString = UtilAll.file2String(fileName);
			//文件不存在，或者为空文件
			if (null == jsonString || jsonString.length() == 0) {
				return this.loadBak();
			} else {
				this.decode(jsonString);
				LOG.info("load " + fileName + " OK");
				return true;
			}
		} catch (Exception e) {
			LOG.severe("load " + this.getClass().getSimpleName() + " Failed" + e);
			return false;
		}
	}
    
    /**
     * 加载备份配置文件
     * 
     * @return
     */
    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = getConsumerOffsetPath();
            String jsonString = UtilAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                LOG.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            LOG.severe("load " + fileName + " Failed" + e);
            return false;
        }
        return true;
    }
    
    /**
     * 将配置对象JSON持久到本地
     */
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = getConsumerOffsetPath();
            try {
            	UtilAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                LOG.severe("persist file Exception, " + fileName + e);
            }
        }
    }
}
