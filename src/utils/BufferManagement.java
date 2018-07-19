package utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author RAMA NARAYAN LAKSHMANAN
 *
 */
public class BufferManagement {

	/**
	 * @param stringValue
	 * @return bytes
	 */
	public static byte[] getStringInBytes(String stringValue) {
		return stringValue.getBytes();
	}

	/**
	 * @param intValue
	 * @return bytes
	 */
	public static byte[] getIntInBytes(int intValue) {
		ByteBuffer intBuffer = ByteBuffer.allocate(4);
		intBuffer.putInt(intValue);
		byte[] byteBufferContent = intBuffer.array();
		intBuffer.clear();
		return byteBufferContent;
	}

	/**
	 * @param shortValue
	 * @return bytes
	 */
	public static byte[] getShortInBytes(short shortValue) {
		ByteBuffer shortBuffer = ByteBuffer.allocate(2);
		shortBuffer.putShort(shortValue);
		byte[] byteBufferContent = shortBuffer.array();
		shortBuffer.clear();
		return byteBufferContent;
	}

	/**
	 * @param longValue
	 * @return bytes
	 */
	public static byte[] getLongInBytes(long longValue) {
		ByteBuffer longBuffer = ByteBuffer.allocate(8);
		longBuffer.putLong(longValue);
		byte[] byteBufferContent = longBuffer.array();
		longBuffer.clear();
		return byteBufferContent;
	}

	/**
	 * @param floatValue
	 * @return bytes
	 */
	public static byte[] getFloatInBytes(float floatValue) {
		ByteBuffer floatBuffer = ByteBuffer.allocate(4);
		floatBuffer.putFloat(floatValue);
		byte[] byteBufferContent = floatBuffer.array();
		floatBuffer.clear();
		return byteBufferContent;
	}

	/**
	 * @param doubleValue
	 * @return bytes
	 */
	public static byte[] getDoubleInBytes(double doubleValue) {
		ByteBuffer doubleBuffer = ByteBuffer.allocate(8);
		doubleBuffer.putDouble(doubleValue);
		byte[] byteBufferContent = doubleBuffer.array();
		doubleBuffer.clear();
		return byteBufferContent;
	}

	/**
	 * @param bytesValue
	 * @return short
	 */
	public static short getBytesInShort(byte[] bytesValue) {
		ByteBuffer shortBuffer = ByteBuffer.allocate(2);
		for (int i = 0; i < 2; i++) {
			shortBuffer.put(bytesValue[i]);
		}
		shortBuffer.clear();
		short bufferValue = shortBuffer.getShort();
		shortBuffer.clear();
		return bufferValue;

	}

	/**
	 * @param bytesValue
	 * @return int
	 */ 
	public static int getBytesInInt(byte[] bytesValue) {
		ByteBuffer intBuffer = ByteBuffer.allocate(4);
		for (int i = 0; i < 4; i++) {
			intBuffer.put(bytesValue[i]);
		}
		intBuffer.clear();
		int bufferValue = intBuffer.getInt();
		intBuffer.clear();
		return bufferValue;
	}

	/**
	 * @param bytesValue
	 * @return long
	 */
	public static long getBytesInLong(byte[] bytesValue) {
		ByteBuffer longBuffer = ByteBuffer.allocate(8);
		for (int i = 0; i < 8; i++) {
			longBuffer.put(bytesValue[i]);
		}
		longBuffer.clear();
		long bufferValue = longBuffer.getLong();
		longBuffer.clear();
		return bufferValue;

	}

	/**
	 * @param bytesValue
	 * @return float
	 */
	public static float getBytesInFloat(byte[] bytesValue) {
		ByteBuffer floatBuffer = ByteBuffer.allocate(4);
		for (int i = 0; i < 4; i++) {
			floatBuffer.put(bytesValue[i]);
		}
		floatBuffer.clear();
		float bufferValue = floatBuffer.getFloat();
		floatBuffer.clear();
		return bufferValue;
	}

	/**
	 * @param bytesValue
	 * @return double
	 */
	public static double getBytesInDouble(byte[] bytesValue) {
		ByteBuffer doubleBuffer = ByteBuffer.allocate(8);
		for (int i = 0; i < 8; i++) {
			doubleBuffer.put(bytesValue[i]);
		}
		doubleBuffer.clear();
		double bufferValue = doubleBuffer.getDouble();
		doubleBuffer.clear();
		return bufferValue;
	}

	/**
	 * @param bytesValue
	 * @return string
	 */
	public static String getBytesInString(byte[] bytesValue) {
		String stringValue = new String(bytesValue);
		return stringValue;
	}

	/**
	 * @param node
	 * @param offset
	 * @return byte
	 */
	public static byte read(byte[] node, int offset) {
		return node[offset];
	}

	/**
	 * @param node
	 * @param offset
	 * @return int
	 */
	public static int readInt(byte[] node, int offset) {
		byte[] intBuffer = new byte[4];
		for (int i = 0; i < 4; i++) {
			intBuffer[i] = node[offset];
			offset = offset + 1;
		}
		int intValue = BufferManagement.getBytesInInt(intBuffer);
		return intValue;
	}

	/**
	 * @param node
	 * @param offset
	 * @return short
	 */
	public static short readShort(byte[] node, int offset) {
		byte[] shortBuffer = new byte[2];
		for (int i = 0; i < 2; i++) {
			shortBuffer[i] = node[offset];
			offset = offset + 1;
		}
		short shortValue = BufferManagement.getBytesInShort(shortBuffer);
		return shortValue;
	}

	/**
	 * @param node
	 * @param offset
	 * @return long
	 */
	public static long readLong(byte[] node, int offset) {
		byte[] longBuffer = new byte[8];
		for (int i = 0; i < 8; i++) {
			longBuffer[i] = node[offset];
			offset = offset + 1;
		}
		long longValue = BufferManagement.getBytesInLong(longBuffer);
		return longValue;
	}

	/**
	 * @param node
	 * @param offset
	 * @return float
	 */
	public static float readFloat(byte[] node, int offset) {
		byte[] floatBuffer = new byte[4];
		for (int i = 0; i < 4; i++) {
			floatBuffer[i] = node[offset];
			offset = offset + 1;
		}
		float floatValue = BufferManagement.getBytesInFloat(floatBuffer);
		return floatValue;
	}

	/**
	 * @param node
	 * @param offset
	 * @return double
	 */
	public static double readDouble(byte[] node, int offset) {
		byte[] doubleBuffer = new byte[8];
		for (int i = 0; i < 8; i++) {
			doubleBuffer[i] = node[offset];
			offset = offset + 1;
		}
		double doubleValue = BufferManagement.getBytesInDouble(doubleBuffer);
		return doubleValue;
	}

	/**
	 * @param node
	 * @param offset
	 * @param stringValue
	 * @return byte[]
	 */
	public static byte[] writeString(byte[] node, int offset, String stringValue) {
		byte[] stringBytes = getStringInBytes(stringValue);
		for (int i = 0; i < stringBytes.length; i++) {
			node[offset] = stringBytes[i];
			offset = offset + 1;
		}
		node[offset] = '\r';
		return node;
	}

	/**
	 * @param node
	 * @param offset
	 * @param intValue
	 * @return byte[]
	 */
	public static byte[] writeInt(byte[] node, int offset, int intValue) {
		byte[] intBytes = getIntInBytes(intValue);
		for (int i = 0; i < 4; i++) {
			node[offset] = intBytes[i];
			offset = offset + 1;
		}
		return node;
	}

	/**
	 * @param node
	 * @param offset
	 * @param shortValue
	 * @return byte[]
	 */
	public static byte[] writeShort(byte[] node, int offset, short shortValue) {
		byte[] shortBytes = getShortInBytes(shortValue);
		for (int i = 0; i < 2; i++) {
			node[offset] = shortBytes[i];
			offset = offset + 1;
		}
		return node;
	}

	/**
	 * @param node
	 * @param offset
	 * @param floatValue
	 * @return byte[]
	 */
	public static byte[] writeFloat(byte[] node, int offset, float floatValue) {
		byte[] floatBytes = getFloatInBytes(floatValue);
		for (int i = 0; i < 4; i++) {
			node[offset] = floatBytes[i];
			offset = offset + 1;
		}
		return node;
	}

	/**
	 * @param node
	 * @param offset
	 * @param doubleValue
	 * @return byte[]
	 */
	public static byte[] writeDouble(byte[] node, int offset, double doubleValue) {
		byte[] doubleBytes = getDoubleInBytes(doubleValue);
		for (int i = 0; i < 8; i++) {
			node[offset] = doubleBytes[i];
			offset = offset + 1;
		}
		return node;
	}

	/**
	 * @param node
	 * @param offset
	 * @param longValue
	 * @return byte[]
	 */
	public static byte[] writeLong(byte[] node, int offset, long longValue) {
		byte[] longBytes = getLongInBytes(longValue);
		for (int i = 0; i < 8; i++) {
			node[offset] = longBytes[i];
			offset = offset + 1;
		}
		return node;
	}

	/**
	 * @param node
	 * @param offset
	 * @param byteValue
	 * @return byte[]
	 */
	public static byte[] write(byte[] node, int offset, byte byteValue) {
		node[offset] = byteValue;
		return node;
	}

	/**
	 * @param node
	 * @param offset
	 * @return byte[]
	 */
	public static String readString(byte[] node, int offset) {

		ArrayList<Byte> bytes = new ArrayList<Byte>();
		while (node[offset] != '\r') {
			bytes.add(node[offset]);
			offset = offset + 1;
		}
		Byte[] stringBytes = bytes.toArray(new Byte[bytes.size()]);
		byte[] stringBytesUnboxed = new byte[stringBytes.length];
		int j = 0;
		for (Byte b : stringBytes)
			stringBytesUnboxed[j++] = b.byteValue();
		String stringValue = getBytesInString(stringBytesUnboxed);
		return stringValue;
	}
}
