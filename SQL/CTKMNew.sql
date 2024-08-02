select * from RT_PProgs where Effect_Date>='2024-04-29 00:00:00.000'  


--select data promotion
select a.Trans_Date as Date,convert(datetime,left(EfTran_Date,12),101) as EfDate,a.StkClust_ID-1 as store,a.Trans_No,a.Goods_ID,Short_Name,Goods_Qty,Goods_Qty/2 as ComboQty,Promotion_Code from 
STr_SaleSet a inner join Str_Hdr b on a.Trans_No=b.Trans_No
inner join goods c on a.Goods_ID=c.Goods_ID
join DSMART90.dbo.RT_PProgs d on a.PProg_ID=d.PProg_ID
where a.PProg_ID in (5722

) --and a.Disabled=1
order by EfTran_Date,Trans_No,a.StkClust_ID asc


--1 san pham
select * from RT_ProgPSItem where PSItem_ID=210260  -- nhap ma item
-- tim dc ma chuong trinh , pprog_id
select * from RT_PProgs where Promotion_Code='201100000901'

--check mã item
select * from GSetItems where GSItem_ID=210873






--select data promotion
select a.Trans_Date as Date,convert(datetime,left(EfTran_Date,12),101) as EfDate,a.StkClust_ID-1 as store,a.Trans_No,a.Goods_ID,Short_Name,Goods_Qty,Goods_Qty/2 as ComboQty from 
STr_SaleSet a inner join str_hdr b on a.Trans_No=b.Trans_No
inner join goods c on a.Goods_ID=c.Goods_ID
where PProg_ID = and a.Disabled=0  and a.trans_date >='2023-02-01' and a.trans_date<'2023-02-01'
order by EfTran_Date,Trans_No,a.StkClust_ID asc

--check discount 
select promotion_code,Prom_Description
,Direct_Disc_Amt
from STr_SaleDtl a inner join
(select trans_date,trans_no,a.pprog_id,goods_id,sum(goods_qty) as Qty from STr_SaleSet a inner join RT_PProgs b on a.PProg_ID=b.PProg_ID
where  a.Disabled=0 --and Trans_Date>='2022-04-04' and trans_date<'2022-05-01'
and a.PProg_ID=2898
group by trans_date,trans_no,a.pprog_id,goods_id) b on a.Trans_No=b.Trans_No and a.Goods_ID=b.Goods_ID
inner join RT_PProgs c on b.PProg_ID=c.PProg_ID
inner join goods d on a.Goods_ID=d.Goods_ID
where Direct_Disc_Amt<>0 and a.Disabled=0 and StkTrCls_ID=3032
order by a.trans_date,stk_id,a.trans_no asc


select * from str_hdr where random_code like '%jPb6' and trans_date='2022-02-19'

select * from STr_SaleDtl where Trans_No='00240020212202003249'


select * from RT_PProgs where Promotion_Code='201100000081'