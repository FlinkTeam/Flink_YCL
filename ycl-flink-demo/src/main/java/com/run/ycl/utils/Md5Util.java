package com.run.ycl.utils;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;

/**
 * md5加密
 * @author zhangjunwei
 *
 */
public class Md5Util {
	static char hexdigits[]={'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
	
	/**
	 * md5文件加密(文件映射方式)
	 * @param file
	 * @return
	 */
	public static String getMd5ByFile(File file){
		String value=null;
		FileInputStream in=null;
		try {
			in=new FileInputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			MappedByteBuffer byteBuffer=in.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
			MessageDigest md5=MessageDigest.getInstance("MD5");
			md5.update(byteBuffer);
			BigInteger bi=new BigInteger(1,md5.digest());
			value=bi.toString(16);
			byteBuffer.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(in!=null){
				try {
					in.close();	
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return value;
	}
	
	/**
	 * md5文件加密，io方式
	 * @param file
	 * @return
	 */
	public static String getMd5(File file){
		FileInputStream fis=null;
		try {
			fis=new FileInputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			MessageDigest md5=MessageDigest.getInstance("MD5");
			byte[] buffer=new byte[2048];
			int length=-1;
			while((length=fis.read(buffer))!=-1){
				md5.update(buffer,0,length);
			}
			byte[] b=md5.digest();
			return byteToHexString(b);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}finally{
			if(fis!=null){
				try {
					fis.close();	
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 把byte[]数组转换成十六进制字符串表示形式
	 * @param tmp
	 * @return
	 */
	private static String byteToHexString(byte[] tmp){
		String s=null;
		char str[]=new char[16*2];
		int k=0;
		for(int i=0;i<16;i++){
			byte byte0=tmp[i];
			str[k++]=hexdigits[byte0>>>4 & 0xf];
			str[k++]=hexdigits[byte0 & 0xf];
		}
		s=new String(str);
		return s;
	}

	/**
	 * 字符串进行MD5.
	 * @param source
	 * @return
	 */
	public static String getMd5(String source) {
		String result = null;
		try {
			result = source;
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			md5.update(result.getBytes("UTF-8"));
			byte[] b = md5.digest();
			result = byteToHexString(b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
