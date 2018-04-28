package com.JDBC.SQL;

import java.sql.*;
//import java.util.Calendar;
import java.util.Date;
import java.text.*;

public class sqltest {
	private Connection con;           //数据库连接变量con
	public static void main(String[] args){
		sqltest test=new sqltest();    //创建test对象
		Connection con=test.getConnection();//定义一个数据连接
		String sql="select * from employee";//定义一个SQL查询语句
		String sqlinsert="insert into employee values('123456789010','孙博慧','12356','女','南京','5000','1')";
		String sqldelete="delete from employee where Name='对的'";
		//test.getStudent1(sqlinsert);
		test.getStudent(con,sql);
		 Date d = new Date();
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	     System.out.println("当前时间：" + sdf.format(d));
	}
	public void getStudent1(String sql){
		try{
			Statement st=con.createStatement();
			//st.executeUpdate(sql);
			st.execute(sql);
			st.close();
			//con.close();
		}catch(Exception e){
			//e.printStackTrace();
		}
	}
	public void getStudent(Connection con,String sql){   //获取学生方法
		try{
			Statement st=con.createStatement();        //定义一个结果集
			ResultSet rs=st.executeQuery(sql);         //返回结果
			while(rs.next()){  //遍历结果集
				String code1=rs.getString(1);
				String name1=rs.getString(2);
				String age1=rs.getString(3);
				String sexy1=rs.getString(4);
				System.out.println("\n姓名: "+name1+"\t学号: "+code1+"\t年龄: "+age1+"\t性别： "+sexy1);
			}
			st.close();
			con.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public Connection getConnection(){   //连接数据库
		String url1="jdbc:sqlserver://127.0.0.1:1433;DatabaseName=test";
		String name="com.microsoft.sqlserver.jdbc.SQLServerDriver";
		//String url1="jdbc:odbc:test";
		String username="root";
		String password="123456";
		try{
			Class.forName(name);
			//登录到建立的数据源上去，也可以说登录到数据库上去
			con=DriverManager.getConnection(url1,username,password);
		}catch(SQLException e){
			e.printStackTrace();
		}catch(ClassNotFoundException ex){
			ex.printStackTrace();
		}
		return con;   //返回数据库连接
	}
}
