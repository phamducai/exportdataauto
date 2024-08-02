select Stk_ID-1 as Store,convert(datetime,left(EF_Date,12),101) as Date,convert(nvarchar,EF_Date,108) as Time,substring(POCheck_Number,5,8)as PO_number from POCheck
where EF_Date>='2021-03-01 00:00:00.000' and EF_Date<'2021-04-01 00:00:00.000'
order by Tran_Date,Stk_ID asc



