Select 
Item.IMUOM1,
Item.IMLITM,
Item.IMUPMJ,
Item.IMDSC1,
Item.IMDSC2,
Item.IMPRP2,
Item.IMPRP4,
Item.IMPRP6,
Item.IMPRP7,
Item.IMPRP8,
Item.IMITM,
CostCenter.MCMCU,
Branch.IBMCU,
Branch.IBPRP1,
Branch.IBPRP2,
Branch.IBPRP5,
Branch.IBTX,
Branch.IBUPMJ,
Branch.IBUSER,
Branch.IBITM,
ItemCost.COUNCS,
ItemCost.COITM,
'JDE' as RECORD_SOURCE
from 
{{ source('inventory', 'STG_JDE_F4101') }} as Item
left join 
{{ source('inventory', 'STG_JDE_F4102') }} as Branch
on Item.IMITM = Branch.IBITM
left join
{{ source('inventory', 'STG_JDE_F0006') }} as CostCenter
on Branch.IBMCU = CostCenter.MCMCU
left join
{{ source('inventory', 'STG_JDE_F4105') }} as ItemCost
on ItemCost.COITM = Item.IMITM and ItemCost.COITM = CostCenter.MCMCU