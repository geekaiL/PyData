CREATE PROCEDURE Procedure_Name  

    --Procedure_Name为存储过程名(不能以阿拉伯数字开头)，在一个数据库中触发器名是唯一的。名字的长度不能超过个字。PROCEDURE可以简写为PROC。
     
    @Param1 Datatype,@Param2 Datatype 
    
    --@Param1和@Param2为存储过程的参数，Datatype为参数类型,多个参数用逗号隔开,最多允许个参数。
    
AS --存储过程要执行的操作 

BEGIN
    
    --BEGIN跟END组成一个代码块，可以写也可以不写，如果存储过程中执行的SQL语句比较复杂，用BEGIN和END会让代码更加整齐，更容易理解。
 
END
GO --GO就代表结操作完毕　　



exec Procedure_Name [参数名] --调用存储过程Procedure_Name。

drop procedure Procedure_Name --删除存储过程Procedure_Name，不能在一个存储过程中删除另一个存储过程，只能调用另一个存储过程

show procedure status --显示数据库中所有存储的存储过程基本信息，包括所属数据库，存储过程名称，创建时间等

show create procedure Procedure_Name --显示存储过程Procedure_Name的详细信息

exec sp_helptext Procedure_Name --显示你这个Procedure_Name这个对象创建文本