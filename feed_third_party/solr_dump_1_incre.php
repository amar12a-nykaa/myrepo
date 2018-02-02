<?php

if(!isset($_SERVER['HTTP_USER_AGENT']))
{

    require_once dirname(dirname(dirname(__FILE__))) . DIRECTORY_SEPARATOR . 'app' . DIRECTORY_SEPARATOR . 'Mage.php';
    Mage::app();
    $resource = Mage::getSingleton('core/resource');
    $writeConnection = $resource->getConnection('core_write');
    $readConnection = $resource->getConnection('core_read');
    $query3 = "SELECT e.entity_id AS product_id,
    TRIM(e.sku) AS sku,e.type_id AS type_id,
    TRIM(cpevn.value) AS name,LCASE(CONCAT(cpevn.value,\" \",REPLACE(cpevn.value,\" \",\"\"))) AS name_token,
    (CASE WHEN cpeif.value>0 THEN \"Yes\" ELSE \"No\" END) AS fbn, (CASE WHEN at_visibility_default.value=1 THEN \"not_visible\" ELSE \"visible\" END) AS visibility,
    cpev_offer.value as offer_id, null as offer_name,
    (CASE WHEN e.type_id = 'configurable' AND (SELECT attribute_id FROM catalog_product_super_attribute WHERE product_id = `e`.`entity_id`  LIMIT 1) = 587 THEN
    (SELECT CONCAT(COUNT(*),' Shades') FROM catalog_product_super_link AS cpsl LEFT JOIN 
    catalog_product_entity_int AS cpei ON cpsl.product_id = cpei.entity_id AND cpei.store_id = 0 AND cpei.attribute_id = 80 WHERE cpei.value = 1 AND cpsl.parent_id = `e`.`entity_id`)
    WHEN e.type_id = 'configurable' AND (SELECT attribute_id FROM catalog_product_super_attribute WHERE product_id = `e`.`entity_id`  LIMIT 1) = 604 THEN
    (SELECT CONCAT(COUNT(*),' Sizes') FROM catalog_product_super_link AS cpsl LEFT JOIN  
    catalog_product_entity_int AS cpei ON cpsl.product_id = cpei.entity_id AND cpei.store_id = 0 AND cpei.attribute_id = 80 WHERE cpei.value = 1 AND cpsl.parent_id = `e`.`entity_id`)
    WHEN e.type_id = 'configurable' AND (SELECT attribute_id FROM catalog_product_super_attribute WHERE product_id = `e`.`entity_id`  LIMIT 1) = 741 THEN
    (SELECT CONCAT(COUNT(*),' Cities') FROM catalog_product_super_link AS cpsl LEFT JOIN  
    catalog_product_entity_int AS cpei ON cpsl.product_id = cpei.entity_id AND cpei.store_id = 0 AND cpei.attribute_id = 80 WHERE cpei.value = 1 AND cpsl.parent_id = `e`.`entity_id`)
    ELSE cpevpz.value END) AS pack_size,
    (CASE WHEN cpevp.value>0 THEN cpevp.value ELSE 0 END) AS popularity,

    ( CASE WHEN e.type_id = 'bundle' THEN ( SUBSTRING_INDEX(res.rating_count, '/', 1) )
    ELSE ( SELECT COUNT(t2.review_id) FROM review t2  LEFT JOIN
       rating_option_vote t1 ON t1.review_id = t2.review_id WHERE (t2.entity_pk_value = e.entity_id) AND (t2.status_id IN (1, 2))  AND t1.value IS NOT NULL  GROUP BY t2.entity_pk_value ) END ) AS rating,
    REPLACE(cpevbv.value,',' ,'|') AS brands_v1,
    (SELECT VALUE FROM eav_attribute_option_value AS eaov WHERE eaov.option_id = cpeib.value AND eaov.store_id = 0) AS brand,
    CONCAT(\"".Mage::getBaseUrl()."\",cur.request_path) AS product_url,
    (CASE WHEN e.type_id  = 'simple'  THEN CONCAT(\"".Mage::getBaseUrl()."ajaxaddcart/index/add/product/\",e.entity_id,\"/isAjax/1\")  ELSE NULL END) AS add_to_cart_url,
    cpedsf.value AS special_from_date,cpedst.value AS special_to_date,
    ( CASE WHEN e.type_id = 'bundle' THEN res.display_rating ELSE res.rating_summary END ) AS rating_percentage, 
    res.reviews_count AS review_count, 
    ( CASE WHEN e.type_id = 'bundle' THEN ROUND((res.display_rating*5)/100, 1) ELSE ROUND((res.rating_summary*5)/100, 1) END ) AS rating_num,
    csi.stock_status AS is_in_stock, REPLACE(cpevcv.value,',' ,'|') AS color_v1, REPLACE(cpevcnv.value,',' ,'|') AS concern_v1,
    REPLACE(cpevsv.value,',' ,'|') AS spf_v1,REPLACE(cpevstv.value,',' ,'|') AS skin_type_v1, REPLACE(cpevpv.value,',' ,'|') AS preference_v1,
    REPLACE(cpevgv.value,',' ,'|') AS gender_v1, REPLACE(cpevfv.value,',' ,'|') AS formulation_v1,
    REPLACE(cpevfnv.value,',' ,'|') AS finish_v1, REPLACE(cpevov.value,',' ,'|') AS occasion_v1,
    REPLACE(cpevcov.value,',' ,'|') AS coverage_v1, REPLACE(cpevfrv.value,',' ,'|') AS fragrance_v1,
    REPLACE(cpevhtv.value,',' ,'|') AS hair_type_v1, REPLACE(cpevsktv.value,',' ,'|') AS skin_tone_v1,
    REPLACE(cpevbnv.value,',' ,'|') AS benefits_v1, REPLACE(cpevbtv.value,',' ,'|') AS bristle_type_v1,
    cpeiarv.value AS age_range_v1, cpeiftv.value AS fragrance_type_v1, cpeifcv.value AS collection_v1,
    REPLACE(cpevlftv.value,',' ,'|') AS luxury_fragrance_type_v1, 

    ( CASE
          WHEN e.type_id = 'configurable' THEN (
              SELECT Group_concat(cpsl.product_id SEPARATOR '|') from 
              catalog_product_super_link as cpsl 
              LEFT JOIN catalog_product_entity_int AS cpei
                      ON cpsl.product_id = cpei.entity_id
                      WHERE  cpei.attribute_id = 80 AND cpei.value = 1 and cpsl.parent_id = e.entity_id
          )
          WHEN e.type_id = 'simple' THEN (
              SELECT cpsl.parent_id FROM catalog_product_super_link AS cpsl LEFT JOIN catalog_product_entity_int cpei ON cpei.entity_id = cpsl.parent_id WHERE cpei.attribute_id = 80 AND cpei.value = 1 AND cpsl.product_id = e.entity_id GROUP BY cpsl.product_id
          )
          WHEN e.type_id = 'bundle' THEN (
              SELECT Group_concat(cpbs.product_id SEPARATOR '|') from 
              catalog_product_bundle_selection as cpbs 
                      WHERE cpbs.parent_product_id = e.entity_id
          )
    end ) as parent_id, -- new added
    ( CASE
          WHEN e.type_id = 'configurable' THEN (
              SELECT Group_concat(cpep.sku SEPARATOR '|') from catalog_product_entity as cpep 
              LEFT JOIN catalog_product_super_link cpsl on cpep.entity_id = cpsl.product_id
              LEFT JOIN catalog_product_entity_int cpei on cpei.entity_id = cpsl.product_id 
              where cpsl.parent_id = e.entity_id AND cpei.attribute_id = 80 AND cpei.value = 1
          )
          WHEN e.type_id = 'simple' THEN (
              SELECT cpep.sku FROM catalog_product_entity AS cpep JOIN catalog_product_super_link cpsl ON cpep.entity_id = cpsl.parent_id LEFT JOIN catalog_product_entity_int cpei ON cpei.entity_id = cpsl.parent_id WHERE cpei.attribute_id = 80 AND cpei.value = 1 AND cpsl.product_id = e.entity_id GROUP BY cpsl.product_id
          )
          WHEN e.type_id = 'bundle' THEN (
              SELECT Group_concat(cpep.sku SEPARATOR '|') from catalog_product_entity as cpep
              LEFT JOIN catalog_product_bundle_selection cpbs ON cpep.entity_id = cpbs.product_id
                      WHERE cpbs.parent_product_id = e.entity_id
          )
    end ) as parent_sku, -- new added
    ( CASE
               WHEN e.type_id = 'configurable'
                    AND (SELECT attribute_id FROM   catalog_product_super_attribute
                         WHERE  product_id = e . entity_id LIMIT  1) = 587 THEN (SELECT
                Group_concat(eaov_color.option_id SEPARATOR '|')
                                               FROM
               catalog_product_entity_int cpev_color
               LEFT JOIN eav_attribute_option_value
                         eaov_color
                      ON cpev_color.value =
                         eaov_color.option_id
               LEFT JOIN catalog_product_super_link
                         cpsl
                      ON cpev_color.entity_id =
                         cpsl.product_id
               LEFT JOIN catalog_product_entity_int
                         AS
                         cpei
                      ON cpsl.product_id =
                         cpei.entity_id
                         AND cpei.store_id = 0
                                               WHERE  cpev_color.attribute_id = 587
                                                      AND cpei.attribute_id = 80
                                                      AND cpei.value = 1
                                                      AND eaov_color.store_id = 0
                                                      AND cpsl.parent_id =
                                                          e.entity_id)
               WHEN e.type_id = 'simple' THEN (SELECT option_id
                     FROM   eav_attribute_option_value AS eaov
                     WHERE  eaov.option_id IN (SELECT cpev_color.value
                                               FROM   catalog_product_entity_int
                                                      cpev_color
                                               WHERE  cpev_color.attribute_id = 587
                                                      AND cpev_color.entity_id =
                                                          e.entity_id)
                            AND eaov.store_id = 0)
    end )                                                        AS shade_id, -- new added
    ( CASE
               WHEN e.type_id = 'configurable'
                    AND (SELECT attribute_id
                         FROM   catalog_product_super_attribute
                         WHERE  product_id = e . entity_id
                         LIMIT  1) = 587 THEN (SELECT
                Group_concat(SUBSTRING_INDEX(eaov_color.value,'#','-1') SEPARATOR '|')
                                               FROM
               catalog_product_entity_int cpev_color
               LEFT JOIN eav_attribute_option_value
                         eaov_color
                      ON cpev_color.value =
                         eaov_color.option_id
               LEFT JOIN catalog_product_super_link
                         cpsl
                      ON cpev_color.entity_id =
                         cpsl.product_id
               LEFT JOIN catalog_product_entity_int
                         AS
                         cpei
                      ON cpsl.product_id =
                         cpei.entity_id
                         AND cpei.store_id = 0
                                               WHERE  cpev_color.attribute_id = 587
                                                      AND cpei.attribute_id = 80
                                                      AND cpei.value = 1
                                                      AND eaov_color.store_id = 0
                                                      AND cpsl.parent_id =
                                                          e.entity_id)
               WHEN e.type_id = 'simple' THEN (SELECT SUBSTRING_INDEX(value,'#','-1')
                     FROM   eav_attribute_option_value AS eaov
                     WHERE  eaov.option_id IN (SELECT cpev_color.value
                                               FROM   catalog_product_entity_int
                                                      cpev_color
                                               WHERE  cpev_color.attribute_id = 587
                                                      AND cpev_color.entity_id =
                                                          e.entity_id)
                            AND eaov.store_id = 0)
    end )                                                        AS shade_name, -- new added
    ( CASE
               WHEN e.type_id = 'configurable'
                    AND (SELECT attribute_id
                         FROM   catalog_product_super_attribute
                         WHERE  product_id = e . entity_id
                         LIMIT  1) = 587 THEN (SELECT
                Group_concat( CONCAT(\"".Mage::getBaseUrl()."media/icons/\", SUBSTRING_INDEX(LOWER(REPLACE(TRIM(eaov_color.value),' ','%20')),'#',1), '.jpg') SEPARATOR '|' )
                                               FROM
               catalog_product_entity_int cpev_color
               LEFT JOIN eav_attribute_option_value
                         eaov_color
                      ON cpev_color.value =
                         eaov_color.option_id
               LEFT JOIN catalog_product_super_link
                         cpsl
                      ON cpev_color.entity_id =
                         cpsl.product_id
               LEFT JOIN catalog_product_entity_int
                         AS
                         cpei
                      ON cpsl.product_id =
                         cpei.entity_id
                         AND cpei.store_id = 0
                                               WHERE  cpev_color.attribute_id = 587
                                                      AND cpei.attribute_id = 80
                                                      AND cpei.value = 1
                                                      AND eaov_color.store_id = 0
                                                      AND cpsl.parent_id =
                                                          e.entity_id)
               WHEN e.type_id = 'simple' THEN (SELECT 
                CONCAT(\"".Mage::getBaseUrl()."media/icons/\", SUBSTRING_INDEX(LOWER(REPLACE(TRIM(value),' ','%20')),'#',1),'.jpg')
                     FROM   eav_attribute_option_value AS eaov
                     WHERE  eaov.option_id IN (SELECT cpev_color.value
                                               FROM   catalog_product_entity_int
                                                      cpev_color
                                               WHERE  cpev_color.attribute_id = 587
                                                      AND cpev_color.entity_id =
                                                          e.entity_id)
                            AND eaov.store_id = 0)
    end )                                                        AS variant_icon, -- new added
    ( CASE
               WHEN e.type_id = 'configurable'
                    AND (SELECT attribute_id
                         FROM   catalog_product_super_attribute
                         WHERE  product_id = e . entity_id
                         LIMIT  1) = 604 THEN (SELECT
                Group_concat(eaov_size.option_id SEPARATOR '|')
                                               FROM
               catalog_product_entity_int cpev_color
               LEFT JOIN eav_attribute_option_value
                         eaov_size
                      ON cpev_color.value =
                         eaov_size.option_id
               LEFT JOIN catalog_product_super_link
                         cpsl
                      ON cpev_color.entity_id =
                         cpsl.product_id
               LEFT JOIN catalog_product_entity_int
                         AS
                         cpei
                      ON cpsl.product_id =
                         cpei.entity_id
                         AND cpei.store_id = 0
                                               WHERE  cpev_color.attribute_id = 604
                                                      AND cpei.attribute_id = 80
                                                      AND cpei.value = 1
                                                      AND eaov_size.store_id = 0
                                                      AND cpsl.parent_id =
                                                          e.entity_id)
               WHEN e.type_id = 'simple' THEN (SELECT eaov_size.option_id
            FROM   catalog_product_entity_int cpev_size
                   JOIN eav_attribute_option_value eaov_size
                     ON cpev_size.value = eaov_size.option_id
            WHERE  cpev_size.attribute_id = 604
                   AND cpev_size.entity_id = e.entity_id
                   AND eaov_size.store_id = 0)
    end )                                                        AS size_id, -- new added
    ( CASE
               WHEN e.type_id = 'configurable'
                    AND (SELECT attribute_id
                         FROM   catalog_product_super_attribute
                         WHERE  product_id = e . entity_id
                         LIMIT  1) = 604 THEN (SELECT
                Group_concat(eaov_size.value SEPARATOR '|')
                                               FROM
               catalog_product_entity_int cpev_color
               LEFT JOIN eav_attribute_option_value
                         eaov_size
                      ON cpev_color.value =
                         eaov_size.option_id
               LEFT JOIN catalog_product_super_link
                         cpsl
                      ON cpev_color.entity_id =
                         cpsl.product_id
               LEFT JOIN catalog_product_entity_int
                         AS
                         cpei
                      ON cpsl.product_id =
                         cpei.entity_id
                         AND cpei.store_id = 0
                                               WHERE  cpev_color.attribute_id = 604
                                                      AND cpei.attribute_id = 80
                                                      AND cpei.value = 1
                                                      AND eaov_size.store_id = 0
                                                      AND cpsl.parent_id =
                                                          e.entity_id)
               WHEN e.type_id = 'simple' THEN (SELECT eaov_size.value
            FROM   catalog_product_entity_int cpev_size
                   JOIN eav_attribute_option_value eaov_size
                     ON cpev_size.value = eaov_size.option_id
            WHERE  cpev_size.attribute_id = 604
                   AND cpev_size.entity_id = e.entity_id
                   AND eaov_size.store_id = 0)
    end )                                                        AS size, -- new added

    null AS description, -- new added
    null AS product_use, -- new added
    null AS product_ingredients, -- new added

    cpeid.value AS d_sku, -- new added
    null AS eretailer, -- new added
    null AS redirect_to_parent, -- new added
    null AS is_a_free_sample, -- new added

    null AS video, -- new added
    cpeis.value As shop_the_look_product, -- new added
    null AS shop_the_look_category_tags, -- new added
    null AS shipping_quote, -- new added
    null AS product_expiry, -- new added
    null AS try_it_on, -- new added
    null AS try_it_on_type, -- new added
    null AS bucket_discount_percent, -- new added
    null AS copy_of_sku, -- new added
    null AS qna_count,
    null AS seller_name,
    null AS offer_description,
    null AS style_divas,
    GROUP_CONCAT(DISTINCT(mt.tag_name) SEPARATOR '|') AS highlights,
    at_status_default.value AS status,
    e.attribute_set_id AS attribute_set_id,
    cssi.qty AS qty,
    cssi.backorders AS backorders,
    (CASE WHEN csi.stock_status > 0 THEN 'true' ELSE 'false' END) AS availability,
    null AS offer_count,
    (SELECT eaov.option_id FROM eav_attribute_option_value AS eaov WHERE eaov.option_id = cpeinvg.value AND eaov.store_id = 0) AS veg_nonveg_v1,
    null AS parent_url
     FROM `catalog_product_entity` AS `e`
    INNER JOIN `catalog_product_entity_int` AS `at_status_default` ON (`at_status_default`.`entity_id` = `e`.`entity_id`) AND (`at_status_default`.`attribute_id` = '80') AND `at_status_default`.`store_id` = 0
    INNER JOIN `catalog_product_entity_int` AS `at_visibility_default` ON (`at_visibility_default`.`entity_id` = `e`.`entity_id`) AND (`at_visibility_default`.`attribute_id` = '85') AND `at_visibility_default`.`store_id` = 0
    LEFT JOIN catalog_product_entity_int  AS cpeid ON cpeid.entity_id = e.entity_id AND cpeid.attribute_id = 718 AND cpeid.store_id = 0
    LEFT JOIN catalog_product_entity_int  AS cpeis ON cpeis.entity_id = e.entity_id AND cpeis.attribute_id = 714 AND cpeis.store_id = 0

    LEFT JOIN catalog_product_entity_varchar  AS cpevn ON cpevn.entity_id = e.entity_id AND cpevn.attribute_id = 56 AND cpevn.store_id = 0
    LEFT JOIN catalog_product_entity_int  AS cpeif ON cpeif.entity_id = e.entity_id AND cpeif.attribute_id = 649 AND cpeif.store_id = 0

    LEFT JOIN catalog_product_entity_varchar  AS cpevp ON cpevp.entity_id = e.entity_id AND cpevp.attribute_id = 707 AND cpevp.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevpz ON cpevpz.entity_id = e.entity_id AND cpevpz.attribute_id = 588 AND cpevpz.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevbv ON cpevbv.entity_id = e.entity_id AND cpevbv.attribute_id = 668 AND cpevbv.store_id = 0
    LEFT JOIN catalog_product_entity_int  AS cpeib ON cpeib.entity_id = e.entity_id AND cpeib.attribute_id = 66 AND cpeib.store_id = 0

    LEFT JOIN core_url_rewrite AS cur ON cur.product_id = e.entity_id AND cur.store_id = 0 AND category_id IS NULL AND is_system = 1

    LEFT JOIN catalog_product_entity_datetime  AS cpedsf ON cpedsf.entity_id = e.entity_id AND cpedsf.attribute_id = 62 AND cpedsf.store_id = 0
    LEFT JOIN catalog_product_entity_datetime  AS cpedst ON cpedst.entity_id = e.entity_id AND cpedst.attribute_id = 63 AND cpedst.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevcv ON cpevcv.entity_id = e.entity_id AND cpevcv.attribute_id = 658 AND cpevcv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevcnv ON cpevcnv.entity_id = e.entity_id AND cpevcnv.attribute_id = 656 AND cpevcnv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevsv ON cpevsv.entity_id = e.entity_id AND cpevsv.attribute_id = 662 AND cpevsv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevstv ON cpevstv.entity_id = e.entity_id AND cpevstv.attribute_id = 657 AND cpevstv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevpv ON cpevpv.entity_id = e.entity_id AND cpevpv.attribute_id = 661 AND cpevpv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevgv ON cpevgv.entity_id = e.entity_id AND cpevgv.attribute_id = 655 AND cpevgv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevfv ON cpevfv.entity_id = e.entity_id AND cpevfv.attribute_id = 659 AND cpevfv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevfnv ON cpevfnv.entity_id = e.entity_id AND cpevfnv.attribute_id = 664 AND cpevfnv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevov ON cpevov.entity_id = e.entity_id AND cpevov.attribute_id = 585 AND cpevov.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevcov ON cpevcov.entity_id = e.entity_id AND cpevcov.attribute_id = 663 AND cpevcov.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevfrv ON cpevfrv.entity_id = e.entity_id AND cpevfrv.attribute_id = 660 AND cpevfrv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevhtv ON cpevhtv.entity_id = e.entity_id AND cpevhtv.attribute_id = 677 AND cpevhtv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevsktv ON cpevsktv.entity_id = e.entity_id AND cpevsktv.attribute_id = 665 AND cpevsktv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevbnv ON cpevbnv.entity_id = e.entity_id AND cpevbnv.attribute_id = 666 AND cpevbnv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar  AS cpevbtv ON cpevbtv.entity_id = e.entity_id AND cpevbtv.attribute_id = 667 AND cpevbtv.store_id = 0
    LEFT JOIN catalog_product_entity_int  AS cpeiarv ON cpeiarv.entity_id = e.entity_id AND cpeiarv.attribute_id = 546 AND cpeiarv.store_id = 0
    LEFT JOIN catalog_product_entity_int  AS cpeiftv ON cpeiftv.entity_id = e.entity_id AND cpeiftv.attribute_id = 622 AND cpeiftv.store_id = 0
    LEFT JOIN catalog_product_entity_int  AS cpeifcv ON cpeifcv.entity_id = e.entity_id AND cpeifcv.attribute_id = 621 AND cpeifcv.store_id = 0
    LEFT JOIN catalog_product_entity_varchar AS cpevlftv ON cpevlftv.entity_id = e.entity_id AND cpevlftv.attribute_id = 693 AND cpevlftv.store_id = 0
    LEFT JOIN catalog_product_entity_int AS cpeinvg ON cpeinvg.entity_id = e.entity_id AND cpeinvg.attribute_id = 696 AND cpeinvg.store_id = 0
    LEFT JOIN cataloginventory_stock_status AS csi ON csi.product_id = e.entity_id
    LEFT JOIN cataloginventory_stock_item AS cssi ON cssi.product_id = e.entity_id
    LEFT JOIN review_entity_summary AS res ON res.entity_pk_value = e.entity_id AND res.store_id = 0
    LEFT JOIN  catalog_product_entity_varchar AS cpev_offer ON cpev_offer.entity_id = e.entity_id AND cpev_offer.attribute_id = 678 AND cpev_offer.store_id = 0
    LEFT JOIN manage_tag_sku_count AS mtsc ON mtsc.sku=e.sku AND mtsc.custom_count+mtsc.tag_count  > 0
    LEFT JOIN `manage_tags` AS mt ON mtsc.tag_id = mt.id  AND mt.status = 1 
    WHERE e.updated_at  BETWEEN  '".date("Y-m-d")." 00:00:00 ' AND  '".date("Y-m-d")." 23:59:59 '  and (cpeid.value IS NULL OR cpeid.value = 0 OR cpeid.value = 1) GROUP BY e.entity_id";
//e.updated_at  BETWEEN  '2017-04-11 00:00:00' AND  '2017-06-11  23:59:59'
    $command = $readConnection->query($query3);
    $result = $command->fetchAll();

    
if(!empty($result))
{
  $all_products = count($result);
    foreach ($result as $key => $value) 
    {

      $product_id= "'".$value['product_id']."'";  
      $sku = "'".$value['sku']."'";
      $type_id = "'".$value['type_id']."'";
      $name = "'".$value['name']."'";
      $name_token = "'".$value['name_token']."'";
      $fbn = "'".$value['fbn']."'";
      $visibility = "'".$value['visibility']."'";
      $offer_id = "'".$value['offer_id']."'";
      $offer_name = "'".$value['offer_name']."'";
      $pack_size = "'".$value['pack_size']."'";
      $popularity = "'".$value['popularity']."'";
      $rating = "'".$value['rating']."'";
      $brands_v1 = "'".$value['brands_v1']."'";
      $brand = "'".$value['brand']."'";
      $product_url = "'".$value['product_url']."'";
      $add_to_cart_url = "'".$value['add_to_cart_url']."'";
      $special_from_date = "'".$value['special_from_date']."'";
      $special_to_date = "'".$value['special_to_date']."'";
      $rating_percentage = "'".$value['rating_percentage']."'";
      $review_count = "'".$value['review_count']."'";
      $rating_num = "'".$value['rating_num']."'";
      $is_in_stock = "'".$value['is_in_stock']."'";
      $color_v1 = "'".$value['color_v1']."'";
      $concern_v1 = "'".$value['concern_v1']."'";
      $spf_v1 = "'".$value['spf_v1']."'";
      $skin_type_v1 = "'".$value['skin_type_v1']."'";
      $preference_v1 = "'".$value['preference_v1']."'";
      $gender_v1 = "'".$value['gender_v1']."'";
      $formulation_v1 = "'".$value['formulation_v1']."'";
      $finish_v1 = "'".$value['finish_v1']."'";
      $occasion_v1 = "'".$value['occasion_v1']."'";
      $coverage_v1 = "'".$value['coverage_v1']."'";
      $fragrance_v1 = "'".$value['fragrance_v1']."'";
      $hair_type_v1 = "'".$value['hair_type_v1']."'";
      $skin_tone_v1 = "'".$value['skin_tone_v1']."'";
      $benefits_v1 = "'".$value['benefits_v1']."'";
      $bristle_type_v1 = "'".$value['bristle_type_v1']."'";
      $age_range_v1 = "'".$value['age_range_v1']."'";
      $fragrance_type_v1 = "'".$value['fragrance_type_v1']."'";
      $collection_v1 = "'".$value['collection_v1']."'";
      $luxury_fragrance_type_v1 = "'".$value['luxury_fragrance_type_v1']."'";
      $parent_id = "'".$value['parent_id']."'";
      $parent_sku = "'".$value['parent_sku']."'";
      $shade_id = "'".$value['shade_id']."'";
      $shade_name = "'".$value['shade_name']."'";
      $variant_icon = "'".$value['variant_icon']."'";
      $size_id = "'".$value['size_id']."'";
      $size = "'".$value['size']."'";
      $description = "'".$value['description']."'";
      $product_use = "'".$value['product_use']."'";
      $product_ingredients = "'".$value['product_ingredients']."'";
      $d_sku = "'".$value['d_sku']."'";
      $eretailer = "'".$value['eretailer']."'";
      $redirect_to_parent = "'".$value['redirect_to_parent']."'";
      $is_a_free_sample = "'".$value['is_a_free_sample']."'";
      $video = "'".$value['video']."'";
      $shop_the_look_product = "'".$value['shop_the_look_product']."'";
      $shop_the_look_category_tags = "'".$value['shop_the_look_category_tags']."'";
      $shipping_quote = "'".$value['shipping_quote']."'";
      $product_expiry = "'".$value['product_expiry']."'";
      $try_it_on = "'".$value['try_it_on']."'";
      $try_it_on_type = "'".$value['try_it_on_type']."'";
      $bucket_discount_percent = "'".$value['bucket_discount_percent']."'";
      $copy_of_sku = "'".$value['copy_of_sku']."'";
      $qna_count = "'".$value['qna_count']."'";
      $seller_name = "'".$value['seller_name']."'";
      $offer_description = "'".$value['offer_description']."'";
      $style_divas = "'".$value['style_divas']."'";
      $highlights = "'".$value['highlights']."'";
      $qty = "'".$value['qty']."'";
      $backorders = "'".$value['backorders']."'";
      $offer_count = "'".$value['offer_count']."'";
      $veg_nonveg_v1 = "'".$value['veg_nonveg_v1']."'";
      
      $status="'".$value['status']."'";
      $attribute_set_id = "'".$value['attribute_set_id']."'";
      $availability = "'".$value['availability']."'";
      $parent_url="'skjdfnksldnfm sdfkj sdofk'";//"'".$value['parent_url']."'";
      $ids[] =$product_id;
      

      $sd2_query_values[] = $product_id.",".$sku.",".$type_id.",".$name.",".$name_token.",".$fbn.",".$visibility.",".$offer_id.",".$offer_name.",".$pack_size.",".$popularity.",".$rating.",".$brands_v1.",".$brand.",".$product_url.",".$add_to_cart_url.",".$special_from_date.",".$special_to_date.",".$rating_percentage.",".$review_count.",".$rating_num.",".$is_in_stock.",".$color_v1.",".$concern_v1.",".$spf_v1.",".$skin_type_v1.",".$preference_v1.",".$gender_v1.",".$formulation_v1.",".$finish_v1.",".$occasion_v1.",".$coverage_v1.",".$fragrance_v1.",".$hair_type_v1.",".$skin_tone_v1.",".$benefits_v1.",".$bristle_type_v1.",".$age_range_v1.",".$fragrance_type_v1.",".$collection_v1.",".$luxury_fragrance_type_v1.",".$parent_id.",".$parent_sku.",".$shade_id.",".$shade_name.",".$variant_icon.",".$size_id.",".$size.",".$description.",".$product_use.",".$product_ingredients.",".$d_sku.",".$eretailer.",".$redirect_to_parent.",".$is_a_free_sample.",".$video.",".$shop_the_look_product.",".$shop_the_look_category_tags.",".$shipping_quote.",".$product_expiry.",".$try_it_on.",".$try_it_on_type.",".$bucket_discount_percent.",".$copy_of_sku.",".$qna_count.",".$seller_name.",".$offer_description.",".$style_divas.",".$highlights.",".$status.",".$attribute_set_id.",".$qty.",".$backorders.",".$availability.",".$offer_count.",".$veg_nonveg_v1.",".$parent_url.",";
      /*echo $sd2_query_values;
      exit();*/

    }
 /*   print_r($sd2_query_values);
    exit();*/
    echo $ids1=implode(",",$ids);
    
    $i=0;
    $findids="select id from solr_dump_3 where product_id in (".$ids1.")";
    $command_findids = $readConnection->query($findids);
    $result_findids = $command_findids->fetchAll();
    foreach ($result_findids as $key => $value) 
    {
     // echo $value['id']."\n";

      $query = rtrim($sd2_query_values[$i],",");
      $sd3_update_query .= "('".$value['id']."',".$query."),";
     // exit();

    
    
    //echo $sd3_update_query1;
    //exit();
    if($i % 1000 == 0 || $i >= $all_products){
      $sd3_update_query1=rtrim($sd3_update_query,",");
    $sd2_query = 'INSERT INTO `solr_dump_3` (id,product_id,sku,type_id,name,name_token,fbn,visibility,offer_id,offer_name,pack_size,popularity,rating,brands_v1,brand,product_url,add_to_cart_url,special_from_date,special_to_date,rating_percentage,review_count,rating_num,is_in_stock,color_v1,concern_v1,spf_v1,skin_type_v1,preference_v1,gender_v1,formulation_v1,finish_v1,occasion_v1,coverage_v1,fragrance_v1,hair_type_v1,skin_tone_v1,benefits_v1,bristle_type_v1,age_range_v1,fragrance_type_v1,collection_v1,luxury_fragrance_type_v1,parent_id,parent_sku,shade_id,shade_name,variant_icon,size_id,size,description,product_use,product_ingredients,d_sku,eretailer,redirect_to_parent,is_a_free_sample,video,shop_the_look_product,shop_the_look_category_tags,shipping_quote,product_expiry,try_it_on,try_it_on_type,bucket_discount_percent,copy_of_sku,qna_count,seller_name,offer_description,style_divas,highlights,status,attribute_set_id,qty,backorders,availability,offer_count,veg_nonveg_v1,parent_url) VALUES  '.$sd3_update_query1.' ON DUPLICATE KEY UPDATE product_id=VALUES(product_id),sku=VALUES(sku),type_id=VALUES(type_id),name=VALUES(name),name_token=VALUES(name_token),fbn=VALUES(fbn),visibility=VALUES(visibility),offer_id=VALUES(offer_id),offer_name=VALUES(offer_name),pack_size=VALUES(pack_size),popularity=VALUES(popularity),rating=VALUES(rating),brands_v1=VALUES(brands_v1),brand=VALUES(brand),product_url=VALUES(product_url),add_to_cart_url=VALUES(add_to_cart_url),special_from_date=VALUES(special_from_date),special_to_date=VALUES(special_to_date),rating_percentage=VALUES(rating_percentage),review_count=VALUES(review_count),rating_num=VALUES(rating_num),is_in_stock=VALUES(is_in_stock),color_v1=VALUES(color_v1),concern_v1=VALUES(concern_v1),spf_v1=VALUES(spf_v1),skin_type_v1=VALUES(skin_type_v1),preference_v1=VALUES(preference_v1),gender_v1=VALUES(gender_v1),formulation_v1=VALUES(formulation_v1),finish_v1=VALUES(finish_v1),occasion_v1=VALUES(occasion_v1),coverage_v1=VALUES(coverage_v1),fragrance_v1=VALUES(fragrance_v1),hair_type_v1=VALUES(hair_type_v1),skin_tone_v1=VALUES(skin_tone_v1),benefits_v1=VALUES(benefits_v1),bristle_type_v1=VALUES(bristle_type_v1),age_range_v1=VALUES(age_range_v1),fragrance_type_v1=VALUES(fragrance_type_v1),collection_v1=VALUES(collection_v1),luxury_fragrance_type_v1=VALUES(luxury_fragrance_type_v1),parent_id=VALUES(parent_id),parent_sku=VALUES(parent_sku),shade_id=VALUES(shade_id),shade_name=VALUES(shade_name),variant_icon=VALUES(variant_icon),size_id=VALUES(size_id),size=VALUES(size),description=VALUES(description),product_use=VALUES(product_use),product_ingredients=VALUES(product_ingredients),d_sku=VALUES(d_sku),eretailer=VALUES(eretailer),redirect_to_parent=VALUES(redirect_to_parent),is_a_free_sample=VALUES(is_a_free_sample),video=VALUES(video),shop_the_look_product=VALUES(shop_the_look_product),shop_the_look_category_tags=VALUES(shop_the_look_category_tags),shipping_quote=VALUES(shipping_quote),product_expiry=VALUES(product_expiry),try_it_on=VALUES(try_it_on),try_it_on_type=VALUES(try_it_on_type),bucket_discount_percent=VALUES(bucket_discount_percent),copy_of_sku=VALUES(copy_of_sku),qna_count=VALUES(qna_count),seller_name=VALUES(seller_name),offer_description=VALUES(offer_description),style_divas=VALUES(style_divas),highlights=VALUES(highlights),status=VALUES(status),attribute_set_id=VALUES(attribute_set_id),qty=VALUES(qty),backorders=VALUES(backorders),availability=VALUES(availability),offer_count=VALUES(offer_count),veg_nonveg_v1=VALUES(veg_nonveg_v1),parent_url=VALUES(parent_url);';
//exit();
    $writeConnection->query($sd2_query);
    unset($sd3_update_query1);
    unset($sd2_query);
    }
     $i++;
   
    }
 }else{
    echo "Products are not updated";
  }

}