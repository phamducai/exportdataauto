SELECT
a.Trans_Date,
CONVERT ( datetime, LEFT ( a.EfTran_Date, 12 ), 101 ) AS EfDate,
datepart( hh, CONVERT ( TIME, a.EfTran_Date, 108 ) ) AS HOUR,
a.Trans_No,
a.StkTrCls_ID AS TransCode,
Stk_ID - 1 AS store,
a.Goods_ID,
Goods_Grp_ID AS CategoryID,
SUM ( SKUBase_Qty ) AS Qty,
SUM ( Sales_Amt + Direct_Disc_Amt ) AS Sales,
SUM ( VAT_Amt ) AS VAT_amt,
SUM ( Direct_Disc_Amt ) AS Discount,
SUM ( RT_Price ) AS RT_Price,
Member_No,
Full_Name 
FROM
  DSMART90_ARC.dbo.STr_SaleDtl_202312 a
  JOIN DSMART90.dbo.RT_Price b ON a.Goods_ID= b.Goods_ID
  JOIN DSMART90_ARC.dbo.str_hdr_202312 c ON a.Trans_No= c.Trans_No
  JOIN DSMART90.dbo.Goods d ON a.Goods_ID= d.Goods_ID 
WHERE
  a.Disabled= 0 
  AND a.EfTran_Date>= '2023-12-01' 
  AND a.EfTran_Date< '2024-01-01' -- sua ngay can xuat
  AND Goods_Grp_ID IN ( 905, 999, 1001, 1003, 1005, 1006, 1007, 1088, 1099, 1101 ) -- Nhap ma cate
GROUP BY
  a.Trans_Date,
  CONVERT ( datetime, LEFT ( a.EfTran_Date, 12 ), 101 ),
  datepart( hh, CONVERT ( TIME, a.EfTran_Date, 108 ) ),
  a.Trans_No,
  a.StkTrCls_ID,
  Stk_ID,
  a.Goods_ID,
  Goods_Grp_ID,
  Member_No,
  Full_Name