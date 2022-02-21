Select Distinct
Item.IMUOM1 as ItemUnitOfMeasure_Code ,
Item.IMLITM as SecondItemNumber,
Item.IMUPMJ as SourceLastUpdateDate,
Item.IMDSC1 as ItemDescription1,
Item.IMDSC2 as ItemDescription2,
Item.IMPRP2 as ItemCommoditySubClass_Code,
Item.IMPRP4 as ItemFamilyGroup_Code,
Item.IMPRP6 as ItemDimensionGroup_Code,
Item.IMPRP7 as ItemWarehouseProcessGroup1_Code,
Item.IMPRP8 as ItemWarehouseProcessGroup2_Code,
Item.IMITM as ItemMaster_ItemNumber,
CostCenter.MCMCU as CostcenterID,
Branch.IBMCU as ItemBranch_BusinessUnit,
Branch.IBPRP1 as ItemCommodityClass_Code,
Branch.IBPRP2 as ItemBranch_ItemCommoditySubClass_Code,
Branch.IBPRP5 as ItemInactiveStatus_Code,
Branch.IBTX as TaxFlag,
Branch.IBUPMJ as ItemBranch_SourceLastUpdateDate,
Branch.IBUSER as SourceLastUpdatedBy,
IFF(IBPRP5='', 1, 0) AS IsActive,
Branch.IBITM as ItemBranch_ItemNumber,
ItemCost.COUNCS as UnitCost,
ItemCost.COITM as ItemCost_ItemNumber,
ItemCost.COMCU as ItemCost_BusinessUnit,
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
on ItemCost.COITM = Item.IMITM and ItemCost.COMCU = CostCenter.MCMCU