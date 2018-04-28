package com.JDBC.SQL;

import java.sql.*;
//import java.util.Calendar;
import java.util.Date;
import java.text.*;

public class sqltest {
	private Connection con;           //���ݿ����ӱ���con
	public static void main(String[] args){
		sqltest test=new sqltest();    //����test����
		Connection con=test.getConnection();//����һ����������
		String sql="select * from employee";//����һ��SQL��ѯ���
		String sqlinsert="insert into employee values('123456789010','�ﲩ��','12356','Ů','�Ͼ�','5000','1')";
		String sqldelete="delete from employee where Name='�Ե�'";
		//test.getStudent1(sqlinsert);
		test.getStudent(con,sql);
		 Date d = new Date();
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	     System.out.println("��ǰʱ�䣺" + sdf.format(d));
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
	public void getStudent(Connection con,String sql){   //��ȡѧ������
		try{
			Statement st=con.createStatement();        //����һ�������
			ResultSet rs=st.executeQuery(sql);         //���ؽ��
			while(rs.next()){  //���������
				String code1=rs.getString(1);
				String name1=rs.getString(2);
				String age1=rs.getString(3);
				String sexy1=rs.getString(4);
				System.out.println("\n����: "+name1+"\tѧ��: "+code1+"\t����: "+age1+"\t�Ա� "+sexy1);
			}
			st.close();
			con.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public Connection getConnection(){   //�������ݿ�
		String url1="jdbc:sqlserver://127.0.0.1:1433;DatabaseName=test";
		String name="com.microsoft.sqlserver.jdbc.SQLServerDriver";
		//String url1="jdbc:odbc:test";
		String username="root";
		String password="123456";
		try{
			Class.forName(name);
			//��¼������������Դ��ȥ��Ҳ����˵��¼�����ݿ���ȥ
			con=DriverManager.getConnection(url1,username,password);
		}catch(SQLException e){
			e.printStackTrace();
		}catch(ClassNotFoundException ex){
			ex.printStackTrace();
		}
		return con;   //�������ݿ�����
	}
}
