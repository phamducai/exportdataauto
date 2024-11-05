select 
  FORMAT(
    CONVERT(
      datetime, 
      LEFT(b.EfTran_Date, 12), 
      101
    ), 
    'yyyy-MM'
  ) AS ActualMonth, 
  c.Pmt_Code,
  d.Stk_ID -1 as store,
  SUM(
    CASE 
      WHEN a.StkTrCls_ID = 3032 THEN d.Sales_Amt
      ELSE 0 
    END
  ) as Sales_Amt,
  SUM(
    CASE 
      WHEN a.StkTrCls_ID = 3032 THEN d.VAT_Amt
      ELSE 0 
    END
  ) as VAT_Amt,
  SUM(
    CASE 
      WHEN a.StkTrCls_ID = 3032 THEN d.Direct_Disc_Amt
      ELSE 0 
    END
  ) as Discount
from 
  STr_Payment a 
  join str_hdr b on a.Trans_No = b.Trans_No 
  join PaymentMode c on a.Pmt_ID = c.Pmt_ID
  join STr_SaleDtl d on a.Trans_No = d.Trans_No
where 
  b.EfTran_Date >= '2024-09-01' 
  and b.EfTran_Date < '2024-10-01' 
  and a.StkTrCls_ID = (3032) 
  and a.Disabled = 0
GROUP BY 
  d.Stk_ID,
  c.Pmt_Code,
  FORMAT(CONVERT(datetime, LEFT(b.EfTran_Date, 12), 101), 'yyyy-MM')
ORDER BY Stk_ID