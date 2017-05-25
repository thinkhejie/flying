package io.openmessaging.fly.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * 各种方法大杂烩
 * 
 * @author rain.gy
 * @version $Id: UtilAll.java, v 0.1 2014-11-21 下午04:43:06 rain.gy Exp $
 */
public class UtilAll {
	
	/** Broker本机地址 */
	private static byte[] brokerHost;

	public static final String ENTRY_SPLIT = "" + (char) 1;

	public static final String KEY_VALUE_SPLIT = "" + (char) 2;

	/**
	 * 字节数组转化成16进制形式
	 * 
	 * @param src
	 * @return
	 */
	public static String bytes2string(byte[] src) {
		StringBuilder sb = new StringBuilder();
		if (src == null || src.length <= 0) {
			return null;
		}
		for (int i = 0; i < src.length; i++) {
			int v = src[i] & 0xFF;
			String hv = Integer.toHexString(v);
			if (hv.length() < 2) {
				sb.append(0);
			}
			sb.append(hv.toUpperCase());
		}
		return sb.toString();
	}

	/**
	 * char转化为字节
	 * 
	 * @param c
	 * @return
	 */
	private static byte charToByte(char c) {
		return (byte) "0123456789ABCDEF".indexOf(c);
	}

	/**
	 * 压缩消息
	 * 
	 * @param src
	 * @param level
	 * @return
	 * @throws IOException
	 */
	public static byte[] compress(final byte[] src, final int level) throws IOException {
		byte[] result = src;
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
		java.util.zip.Deflater deflater = new java.util.zip.Deflater(level);
		DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream, deflater);
		try {
			deflaterOutputStream.write(src);
			deflaterOutputStream.finish();
			deflaterOutputStream.close();
			result = byteArrayOutputStream.toByteArray();
		} catch (IOException e) {
			deflater.end();
			throw e;
		} finally {
			try {
				byteArrayOutputStream.close();
			} catch (IOException e) {
				// DO NOTHING
			}

			deflater.end();
		}

		return result;
	}

	/**
	 * 计算耗时操作，单位ms
	 * 
	 * @param beginTime
	 * @return
	 */
	public static long computeEclipseTimeMilliseconds(final long beginTime) {
		return (System.currentTimeMillis() - beginTime);
	}

	/**
	 * 根据数组生成CRC32编码
	 * 
	 * @param array
	 * @return
	 */
	public static final int crc32(byte[] array) {
		if (array != null) {
			return crc32(array, 0, array.length);
		}

		return 0;
	}

	/**
	 * 根据数组生成CRC32编码
	 * 
	 * @param array
	 * @param offset
	 * @param length
	 * @return
	 */
	public static final int crc32(byte[] array, int offset, int length) {
		CRC32 crc32 = new CRC32();
		crc32.update(array, offset, length);
		return (int) (crc32.getValue() & 0x7FFFFFFF);
	}

	/**
	 * 创建消息ID
	 * 
	 * @param topic
	 * @param queueId
	 * @param offset
	 * @return
	 */
	public static String createMessageId(String topic, String queueId, long offset) {
		StringBuilder sb = new StringBuilder();
		sb.append(topic).append(ENTRY_SPLIT).append(queueId).append(ENTRY_SPLIT).append(offset);
		return bytes2string(sb.toString().getBytes(Charset.forName("UTF-8")));
	}

	/**
	 * 获取当前栈信息
	 * 
	 * @return
	 */
	public static String currentStackTrace() {
		StringBuilder sb = new StringBuilder();
		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
		for (StackTraceElement ste : stackTrace) {
			sb.append("\n\t");
			sb.append(ste.toString());
		}

		return sb.toString();
	}

	/**
	 * 解析消息ID
	 * 
	 * @param msgId
	 * @return
	 */
	public static String[] decodeMessageId(String msgId) {
		byte[] bytes = string2bytes(msgId);

		if (bytes != null) {
			String content = new String(bytes, Charset.forName("UTF-8"));
			if (content != null && (!"".equals(content))) {
				return content.split(ENTRY_SPLIT);
			}
		}

		return null;
	}

	/**
	 * 保留string最大字符数
	 * 
	 * @param str
	 * @param size
	 * @return
	 */
	public static String frontStringAtLeast(final String str, final int size) {
		if (str != null) {
			if (str.length() > size) {
				return str.substring(0, size);
			}
		}

		return str;
	}

	/**
	 * 获取Broker本机地址.
	 * 
	 * @return
	 */
	public static byte[] getBrokerHost() {
		return brokerHost;
	}

	/**
	 * 获取磁盘分区空间使用率
	 * 
	 * @param path
	 * @return
	 */
	public static double getDiskSpaceUsgae(final String path) {
		if (null == path || path.isEmpty())
			return -1;

		try {
			File file = new File(path);
			if (!file.exists()) {
				boolean result = file.mkdirs();
				if (!result) {
					// make directory failed
					return -1;
				}
			}

			long totalSpace = file.getTotalSpace();
			long freeSpace = file.getFreeSpace();
			long usedSpace = totalSpace - freeSpace;
			if (totalSpace > 0) {
				return usedSpace / (double) totalSpace;
			}
		} catch (Exception e) {
			return -1;
		}

		return -1;
	}

	/**
	 * 通过反射调用函数
	 * 
	 * @param target
	 * @param methodName
	 * @param args
	 * @return
	 */
	public static Object invoke(final Object target, final String methodName, final Class<?>... args) {
		return AccessController.doPrivileged(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					Method method = method(target, methodName, args);
					method.setAccessible(true);
					return method.invoke(target);
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		});
	}

	/**
	 * 通过反射获取方法
	 * 
	 * @param target
	 * @param methodName
	 * @param args
	 * @return
	 * @throws NoSuchMethodException
	 */
	public static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
		try {
			return target.getClass().getMethod(methodName, args);
		} catch (NoSuchMethodException e) {
			return target.getClass().getDeclaredMethod(methodName, args);
		}
	}

	/**
	 * 将offset转化成字符串形式<br>
	 * 左补零对齐至20位
	 */
	public static String offset2FileName(final long offset) {
		final NumberFormat nf = NumberFormat.getInstance();
		nf.setMinimumIntegerDigits(20);
		nf.setMaximumFractionDigits(0);
		nf.setGroupingUsed(false);
		return nf.format(offset);
	}

	/**
	 * 将string转为日期对象
	 * 
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static Date parseDate(String date, String pattern) {
		SimpleDateFormat df = new SimpleDateFormat(pattern);
		try {
			return df.parse(date);
		} catch (ParseException e) {
			return null;
		}
	}

	/**
	 * 解析队列前缀
	 * 
	 * @param queueId
	 * @return
	 */
	public static Integer parseQueuePreIndex(String queueId) {
		// 解析队列ID
		if (queueId.length() <= 6) {
			throw new IllegalArgumentException("Queue ID length is less than 7! QueueId:" + queueId);
		}
		Integer queuePreIndex = Integer.parseInt(queueId.substring(0, queueId.length() - 6));

		return queuePreIndex;
	}

	/**
	 * 设置Broker的本机地址.
	 * <p>
	 * 仅在Broker初始化时调用一次
	 * </p>
	 *
	 * @param brokerHost
	 */
	public static void setBrokerHost(SocketAddress brokerHost) {
		if (null == UtilAll.brokerHost && brokerHost != null) {
			UtilAll.brokerHost = SocketAddress2ByteBuffer(brokerHost).array();
		}
	}

	/**
	 * 网络地址转化成ByteBuffer8个字节
	 * 
	 * @param socketAddress
	 * @return
	 */
	public static ByteBuffer SocketAddress2ByteBuffer(SocketAddress socketAddress) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(8);
		InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
		byteBuffer.put(inetSocketAddress.getAddress().getAddress());
		byteBuffer.putInt(inetSocketAddress.getPort());
		byteBuffer.flip();
		return byteBuffer;
	}

	/**
	 * 16进制字符串转化成字节数组
	 * 
	 * @param hexString
	 * @return
	 */
	public static byte[] string2bytes(String hexString) {
		if (hexString == null || hexString.equals("")) {
			return null;
		}
		hexString = hexString.toUpperCase();
		int length = hexString.length() / 2;
		char[] hexChars = hexString.toCharArray();
		byte[] d = new byte[length];
		for (int i = 0; i < length; i++) {
			int pos = i * 2;
			d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
		}
		return d;
	}

	/**
	 * 时间转换器
	 * 
	 * @param t
	 *            时间毫秒
	 * @return
	 */
	public static String timeMillisToHumanString(final long t) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(t);
		return String.format("%04d%02d%02d%02d%02d%02d%03d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
				cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE),
				cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND));
	}

	/**
	 * 解压缩
	 * 
	 * @param src
	 * @return
	 * @throws IOException
	 */
	public static byte[] uncompress(final byte[] src) throws IOException {
		byte[] result = src;
		byte[] uncompressData = new byte[src.length];
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
		InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

		try {
			while (true) {
				int len = inflaterInputStream.read(uncompressData, 0, uncompressData.length);
				if (len <= 0) {
					break;
				}
				byteArrayOutputStream.write(uncompressData, 0, len);
			}
			byteArrayOutputStream.flush();
			result = byteArrayOutputStream.toByteArray();
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				byteArrayInputStream.close();
			} catch (IOException e) {
				// DO NOTHING
			}
			try {
				inflaterInputStream.close();
			} catch (IOException e) {
				// DO NOTHING
			}
			try {
				byteArrayOutputStream.close();
			} catch (IOException e) {
				// DO NOTHING
			}
		}

		return result;
	}

	/**
	 * 获取内存映射文件
	 * 
	 * @param buffer
	 * @return
	 */
	public static ByteBuffer viewed(ByteBuffer buffer) {
		String methodName = "viewedBuffer";

		// JDK7中将DirectByteBuffer类中的viewedBuffer方法换成了attachment方法
		Method[] methods = buffer.getClass().getMethods();
		for (int i = 0; i < methods.length; i++) {
			if (methods[i].getName().equals("attachment")) {
				methodName = "attachment";
				break;
			}
		}

		ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
		if (viewedBuffer == null)
			return buffer;
		else
			return viewed(viewedBuffer);
	}
	
	
	/**
     * 从文件读取数据
     * 
     * @param fileName
     * @return
     */
    public static final String file2String(final String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            char[] data = new char[(int) file.length()];
            boolean result = false;

            FileReader fileReader = null;
            try {
                fileReader = new FileReader(file);
                int len = fileReader.read(data);
                result = (len == data.length);
            } catch (IOException e) {
                // e.printStackTrace();
            } finally {
                if (fileReader != null) {
                    try {
                        fileReader.close();
                    } catch (IOException e) {
                        // DO NOTHING
                    }
                }
            }

            if (result) {
                String value = new String(data);
                return value;
            }
        }
        return null;
    }
    
    /**
     * 安全的写文件
     */
    public static final void string2File(final String str, final String fileName) throws IOException {
        // 先写入临时文件
        String tmpFile = fileName + ".tmp";
        string2FileNotSafe(str, tmpFile);

        // 备份之前的文件
        String bakFile = fileName + ".bak";
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            string2FileNotSafe(prevContent, bakFile);
        }

        // 删除正式文件
        File file = new File(fileName);
        file.delete();

        // 临时文件改为正式文件
        file = new File(tmpFile);
        file.renameTo(new File(fileName));
    }
    
    /**
     * 字符写入文件
     * 
     * @param str
     * @param fileName
     * @throws IOException
     */
    public static final void string2FileNotSafe(final String str, final String fileName) throws IOException {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        FileWriter fileWriter = null;

        try {
            fileWriter = new FileWriter(file);
            fileWriter.write(str);
        } catch (IOException e) {
            throw e;
        } finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    throw e;
                }
            }
        }
    }
}
