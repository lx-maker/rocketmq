package org.apache.rocketmq.example.test;

import org.apache.rocketmq.common.constant.PermName;

/**
 * @author liuxin95
 * @date 2022/6/9 17:49
 */
public class Other{

	public static void main( String[] args ){

//		System.out.println( PermName.isInherited( PermName.PERM_READ | PermName.PERM_WRITE ) );
//		System.out.println(PermName.PERM_READ | PermName.PERM_WRITE | PermName.PERM_INHERIT);
		System.out.println(0x1<<2);
		System.out.println(0x1<<1);
		System.out.println(0x1);
		System.out.println(~0x1);
		System.out.println((0x1<<1 | 0x1 )& ~0x1);
	}
}

