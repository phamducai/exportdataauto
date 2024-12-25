With Test (
select id_item,sum(qty)from ChiTietHoadon group by id_item
)