SELECT
        item.*, item_sku.id AS sku_id,
                item_sku.create_date AS sku_create_date,
                item_sku.modify_date AS sku_modify_date,
                item_sku.version AS sku_version,
                item_sku.tenant_id AS sku_tenant_id,
                item_sku.item_id AS sku_item_id,
                item_sku.`code` AS sku_code,
                item_sku.`name` AS sku_name,
                item_sku.del AS sku_del,
                item_sku.note AS sku_note,
                item_sku.`order` AS sku_order,
                item_sku.weight AS sku_weight,
                item_sku.package_point AS sku_package_point,
                item_sku.sales_point AS sku_sales_point,
                item_sku.sales_price AS sku_sales_price,
                item_sku.purchase_price AS sku_purchase_price,
                item_sku.agent_price AS sku_agent_price,
                item_sku.cost_price AS sku_cost_price,
                item_sku.stock_status_id AS sku_stock_status_id,
                item_sku.tax_no AS sku_tax_no,
                item_sku.tax_rate AS sku_tax_rate,
                item_sku.origin_area AS sku_origin_area,
                item_sku.supplier_outerid AS sku_supplier_outerid,
                item_sku.unit_id AS sku_unit_id,
                item_sku.volume AS sku_volume,
                item_sku.pic_url AS sku_pic_url,
                item_sku.length AS sku_length,
                item_sku.width AS sku_width,
                item_sku.height AS sku_height,
                item_sku.lowest_sales_price AS sku_lowest_sales_price,
                item_sku.sale_unit_id AS sku_sale_unit_id,
                item_sku.purchase_unit_id AS sku_purchase_unit_id
FROM
        item
        LEFT JOIN item_sku ON (
        item_sku.item_id = item.id
        AND item_sku.tenant_id = item.tenant_id
        )
