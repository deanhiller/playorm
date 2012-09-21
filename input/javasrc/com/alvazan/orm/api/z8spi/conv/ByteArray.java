package com.alvazan.orm.api.z8spi.conv;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;


public class ByteArray implements Comparable<ByteArray> {
	private byte[] key;
	
	public ByteArray(byte[] key) {
		this.key = key;
	}
	
	public byte[] getKey() {
		return key;
	}
	
	@Override
	public int hashCode() {
		if(key == null)
			return 0;
		
		long hash = 0;
		for(int i = 0; i < key.length;i++) {
			hash += key[i];
		}
		
		return (int) (hash / 2);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ByteArray other = (ByteArray) obj;
		if(key == null && other.key == null)
			return true;
		else if (!Arrays.equals(key, other.key))
			return false;
		return true;
	}
	@Override
	public int compareTo(ByteArray o) {
		byte[] right = o.key;
		byte[] left = this.key;
		
		int min = Math.min(right.length, left.length);
		for(int i = 0; i < min; i++) {
			//very annoying but in java, we need the int for the unsigned byte so we can compare the
			//unsigned bytes...
			int leftUnsignedByte = javaSignedByteToUnsigned(left[i]);
			int rightUnsignedByte = javaSignedByteToUnsigned(right[i]);
			if(leftUnsignedByte < rightUnsignedByte)
				return -1;
			else if(leftUnsignedByte > rightUnsignedByte)
				return 1;
		}

		//if we got here, they are the same if they are the same length
		if(right.length == left.length)
			return 0;
		
		//if we get here, they are the same up to a point, the shorter one wins
		if(left.length < right.length)
			return -1;
		//else right > left length
		return 1;
	}
	
	//Java has no unsigned byte and we need the unsigned byte value so we can compare..
	public static int javaSignedByteToUnsigned(byte b) {
	    return b & 0xFF;
	}
	
	@Override
	public String toString() {
		String asString = asString();
		BigDecimal asDec = StandardConverters.convertFromBytesNoExc(BigDecimal.class, key);
		BigInteger asInt = StandardConverters.convertFromBytesNoExc(BigInteger.class, key);
		return "[asString:"+asString+"\nasDec:"+asDec+"\nasInt:"+asInt+"\n]";
	}

	public String asString() {
		return StandardConverters.convertFromBytesNoExc(String.class, key);
	}
	
	public boolean hasPrefix(byte[] prefix) {
		if(key.length <= prefix.length)
			return false;
		
		for(int i = 0; i < prefix.length;i++) {
			if(key[i] != prefix[i])
				return false;
		}
		return true;
	}
}