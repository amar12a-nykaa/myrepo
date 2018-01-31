<?php

if(!isset($_SERVER['HTTP_USER_AGENT'])){

require_once dirname(dirname(dirname(__FILE__))) . DIRECTORY_SEPARATOR . 'app' . DIRECTORY_SEPARATOR . 'Mage.php';
Mage::app();

$imageBaseUrl = "http://adn-static1.nykaa.com/";

 $resource = Mage::getSingleton('core/resource');
    $writeConnection = $resource->getConnection('core_write');
    $readConnection = $resource->getConnection('core_read');

$query2 = "SELECT e.entity_id AS product_id,
e.created_at AS created_at,
(SELECT GROUP_CONCAT(category_id SEPARATOR '|') FROM catalog_category_product WHERE product_id = e.entity_id AND category_id != 1) AS category_id,
(SELECT GROUP_CONCAT(TRIM(VALUE) SEPARATOR '|') FROM catalog_category_entity_varchar WHERE entity_id IN (SELECT category_id FROM catalog_category_product WHERE product_id = e.entity_id AND category_id != 1) AND attribute_id = 31) AS category,
null AS category_token,
( SELECT GROUP_CONCAT(CONCAT(\"".Mage::getBaseUrl()."media/catalog/product\", `main`.`value`) SEPARATOR '|') FROM `catalog_product_entity_media_gallery` AS `main`
LEFT JOIN `catalog_product_entity_media_gallery_value` AS `value` ON main.value_id = value.value_id AND value.store_id = 0
WHERE (main.attribute_id = '73') AND (main.entity_id = e.entity_id) AND (value.disabled = 0) ) AS image_url,
( CASE WHEN e.type_id = 'bundle' THEN
(SELECT SUM(cpbs.selection_qty*cped.value) FROM catalog_product_bundle_selection AS cpbs
LEFT JOIN catalog_product_entity_decimal AS cped
ON cpbs.product_id = cped.entity_id AND cped.store_id = 0 AND cped.attribute_id = 60
WHERE cpbs.parent_product_id = e.entity_id)
ELSE cpedp.value END) AS price,
( CASE WHEN e.type_id = 'bundle' THEN
(cpedsp.value/100)*(SELECT SUM(cpbs.selection_qty*cped.value) FROM catalog_product_bundle_selection AS cpbs
LEFT JOIN catalog_product_entity_decimal AS cped
ON cpbs.product_id = cped.entity_id AND cped.store_id = 0 AND cped.attribute_id = 60
WHERE cpbs.parent_product_id = e.entity_id)
ELSE cpedsp.value END
) AS special_price,
0 AS discount,
(SELECT VALUE FROM eav_attribute_option_value AS eaov WHERE eaov.option_id = cpevtag.value AND eaov.store_id = 0) AS tag,
(CASE WHEN e.type_id = 'configurable' AND (SELECT attribute_id FROM catalog_product_super_attribute WHERE product_id = `e`.`entity_id`  LIMIT 1) = 587 THEN 
(SELECT CONCAT(COUNT(*),' Shades') FROM catalog_product_super_link AS cpsl LEFT JOIN 
catalog_product_entity_int AS cpei ON cpsl.product_id = cpei.entity_id AND cpei.store_id = 0 AND cpei.attribute_id = 80 WHERE cpei.value = 1 AND cpsl.parent_id = `e`.`entity_id`)
WHEN e.type_id = 'configurable' AND (SELECT attribute_id FROM catalog_product_super_attribute WHERE product_id = `e`.`entity_id`  LIMIT 1) = 604 THEN
(SELECT CONCAT(COUNT(*),' Sizes') FROM catalog_product_super_link AS cpsl LEFT JOIN  
catalog_product_entity_int AS cpei ON cpsl.product_id = cpei.entity_id AND cpei.store_id = 0 AND cpei.attribute_id = 80 WHERE cpei.value = 1 AND cpsl.parent_id = `e`.`entity_id`)
ELSE cpevpz.value END) AS pack_size,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.color_v1, '|', numbers.n), '|', -1) color_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.color_v1)-CHAR_LENGTH(REPLACE(sd3.color_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS color,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.concern_v1, '|', numbers.n), '|', -1) concern_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.concern_v1)-CHAR_LENGTH(REPLACE(sd3.concern_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS concern,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.spf_v1, '|', numbers.n), '|', -1) spf_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.spf_v1)-CHAR_LENGTH(REPLACE(sd3.spf_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS spf,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.skin_type_v1, '|', numbers.n), '|', -1) skin_type_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.skin_type_v1)-CHAR_LENGTH(REPLACE(sd3.skin_type_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS skin_type,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.preference_v1, '|', numbers.n), '|', -1) preference_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.preference_v1)-CHAR_LENGTH(REPLACE(sd3.preference_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS preference,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.gender_v1, '|', numbers.n), '|', -1) gender_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.gender_v1)-CHAR_LENGTH(REPLACE(sd3.gender_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS gender,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.formulation_v1, '|', numbers.n), '|', -1) formulation_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.formulation_v1)-CHAR_LENGTH(REPLACE(sd3.formulation_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS formulation,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.finish_v1, '|', numbers.n), '|', -1) finish_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.finish_v1)-CHAR_LENGTH(REPLACE(sd3.finish_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS finish,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.occasion_v1, '|', numbers.n), '|', -1) occasion_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.occasion_v1)-CHAR_LENGTH(REPLACE(sd3.occasion_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS occasion,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.coverage_v1, '|', numbers.n), '|', -1) coverage_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.coverage_v1)-CHAR_LENGTH(REPLACE(sd3.coverage_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS coverage,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.fragrance_v1, '|', numbers.n), '|', -1) fragrance_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.fragrance_v1)-CHAR_LENGTH(REPLACE(sd3.fragrance_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS fragrance,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.hair_type_v1, '|', numbers.n), '|', -1) hair_type_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.hair_type_v1)-CHAR_LENGTH(REPLACE(sd3.hair_type_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS hair_type,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.skin_tone_v1, '|', numbers.n), '|', -1) skin_tone_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.skin_tone_v1)-CHAR_LENGTH(REPLACE(sd3.skin_tone_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS skin_tone,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.benefits_v1, '|', numbers.n), '|', -1) benefits_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.benefits_v1)-CHAR_LENGTH(REPLACE(sd3.benefits_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS benefits,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.bristle_type_v1, '|', numbers.n), '|', -1) bristle_type_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.bristle_type_v1)-CHAR_LENGTH(REPLACE(sd3.bristle_type_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS bristle_type,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.luxury_fragrance_type_v1, '|', numbers.n), '|', -1) luxury_fragrance_type_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.luxury_fragrance_type_v1)-CHAR_LENGTH(REPLACE(sd3.luxury_fragrance_type_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS luxury_fragrance_type,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.age_range_v1, '|', numbers.n), '|', -1) age_range_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.age_range_v1)-CHAR_LENGTH(REPLACE(sd3.age_range_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS age_range,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.fragrance_type_v1, '|', numbers.n), '|', -1) fragrance_type_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.fragrance_type_v1)-CHAR_LENGTH(REPLACE(sd3.fragrance_type_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS fragrance_type,
(SELECT REPLACE(GROUP_CONCAT(VALUE),',' ,'|')FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(sd3.collection_v1, '|', numbers.n), '|', -1) collection_v1 FROM (SELECT  @N := @N +1 AS n FROM solr_dump_3, (SELECT @N:=0) len LIMIT 100) numbers INNER JOIN solr_dump_3 sd3
  ON CHAR_LENGTH(sd3.collection_v1)-CHAR_LENGTH(REPLACE(sd3.collection_v1, '|', '')) >= numbers.n-1 WHERE sd3.product_id =  e.entity_id) AND eaov.store_id = 0) AS collection,
cpeisubs.value is_subscribable,
CONCAT(\"".$imageBaseUrl."media/catalog/product/tr:h-800,w-800,cm-pad_resize\", `cpevimg`.`value`) AS main_image,
0 AS show_wishlist_button, 'ADD TO BAG' AS button_text,
(SELECT GROUP_CONCAT(parent_id SEPARATOR '|') FROM catalog_category_entity WHERE entity_id IN (SELECT category_id FROM catalog_category_product WHERE product_id = e.entity_id AND category_id != 1)) AS parent_category_id,
NULL as brands_v1,
NULL as brands,
( SELECT GROUP_CONCAT(CONCAT(\"".$imageBaseUrl."media/catalog/product/tr:h-800,w-800,cm-pad_resize\", `main`.`value`) SEPARATOR '|') FROM `catalog_product_entity_media_gallery` AS `main`
LEFT JOIN `catalog_product_entity_media_gallery_value` AS `value` ON main.value_id = value.value_id AND value.store_id = 0
WHERE (main.attribute_id = '73') AND (main.entity_id = e.entity_id) AND (value.disabled = 0) ) AS image_url_utility,
NULL AS l4_category,
cpetfp.value AS featured_in_urls,
NULL AS featured_in_title,
(SELECT eas.attribute_set_name FROM eav_attribute_set AS eas WHERE eas.entity_type_id = 4 AND eas.attribute_set_id = e.attribute_set_id) AS attribute_set_name,
(SELECT group_concat(value separator '|') FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 3 AND ccpi.category_id != 2 AND cce.level = 2) AND attribute_id = 31) AS categorylevel1,
(SELECT value FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 3 AND ccpi.category_id != 2 AND cce.level = 2) AND attribute_id = 31 limit 1) AS catlevel1name,
(SELECT group_concat(value separator '|') FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 3 AND ccpi.category_id != 2 AND cce.level = 3) AND attribute_id = 31) AS categorylevel2,
(SELECT value FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 3 AND ccpi.category_id != 2 AND cce.level = 3) AND attribute_id = 31 limit 1) AS catlevel2name,
(SELECT group_concat(value separator '|') FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 3 AND ccpi.category_id != 2 AND cce.level = 4) AND attribute_id = 31) AS categorylevel3,
(SELECT value FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 3 AND ccpi.category_id != 2 AND cce.level = 4) AND attribute_id = 31 limit 1) AS catlevel3name,
(SELECT group_concat(value separator '|') FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 3 AND ccpi.category_id != 2 AND cce.level = 5) AND attribute_id = 31) AS categorylevel4,
(SELECT value FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 3 AND ccpi.category_id != 2 AND cce.level = 5) AND attribute_id = 31 limit 1) AS catlevel4name,
(SELECT value FROM eav_attribute_option_value AS eaov WHERE eaov.option_id = cpeinvg.value AND eaov.store_id = 0) AS veg_nonveg
FROM `catalog_product_entity` AS `e`
INNER JOIN `catalog_product_entity_int` AS `at_status_default` ON (`at_status_default`.`entity_id` = `e`.`entity_id`) AND (`at_status_default`.`attribute_id` = '80') AND `at_status_default`.`store_id` = 0
INNER JOIN `catalog_product_entity_int` AS `at_visibility_default` ON (`at_visibility_default`.`entity_id` = `e`.`entity_id`) AND (`at_visibility_default`.`attribute_id` = '85') AND `at_visibility_default`.`store_id` = 0
LEFT JOIN catalog_product_entity_int  AS cpeid ON cpeid.entity_id = e.entity_id AND cpeid.attribute_id = 718 AND cpeid.store_id = 0
LEFT JOIN catalog_product_entity_int  AS cpeis ON cpeis.entity_id = e.entity_id AND cpeis.attribute_id = 714 AND cpeis.store_id = 0

LEFT JOIN catalog_product_entity_decimal AS cpedp ON cpedp.entity_id = e.entity_id AND cpedp.attribute_id = 60 AND cpedp.store_id = 0
LEFT JOIN catalog_product_entity_decimal AS cpedsp ON cpedsp.entity_id = e.entity_id AND cpedsp.attribute_id = 61 AND cpedsp.store_id = 0
#LEFT JOIN catalog_product_entity_int  AS cpeiar ON cpeiar.entity_id = e.entity_id AND cpeiar.attribute_id = 546 AND cpeiar.store_id = 0
#LEFT JOIN catalog_product_entity_int  AS cpeift ON cpeift.entity_id = e.entity_id AND cpeift.attribute_id = 622 AND cpeift.store_id = 0
#LEFT JOIN catalog_product_entity_int  AS cpeifc ON cpeifc.entity_id = e.entity_id AND cpeifc.attribute_id = 621 AND cpeifc.store_id = 0
LEFT JOIN catalog_product_entity_varchar  AS cpevpz ON cpevpz.entity_id = e.entity_id AND cpevpz.attribute_id = 588 AND cpevpz.store_id = 0
LEFT JOIN catalog_product_entity_int AS cpeisubs ON cpeisubs.entity_id = e.entity_id AND cpeisubs.attribute_id = 790 AND cpeisubs.store_id = 0
LEFT JOIN catalog_product_entity_varchar  AS cpevimg ON cpevimg.entity_id = e.entity_id AND cpevimg.attribute_id = 70 AND cpevimg.store_id = 0
LEFT JOIN catalog_product_entity_varchar AS cpevtag ON cpevtag.entity_id = e.entity_id AND cpevtag.attribute_id = 706 AND cpevtag.store_id = 0
LEFT JOIN catalog_product_entity_text  AS cpetfp ON cpetfp.entity_id = e.entity_id AND cpetfp.attribute_id = 798 AND cpetfp.store_id = 0
LEFT JOIN catalog_product_entity_int AS cpeinvg ON cpeinvg.entity_id = e.entity_id AND cpeinvg.attribute_id = 696 AND cpeinvg.store_id = 0 where e.updated_at  BETWEEN  '".date("Y-m-d")." 00:00:00 ' AND  '".date("Y-m-d")." 23:59:59'";
//e.updated_at  BETWEEN  '2017-04-11 00:00:00' AND  '2017-06-11  23:59:59'
//


$command1 = $readConnection->query($query2);
$result = $command1->fetchAll();

if(!empty($result))
{
  $all_products = count($result_cat);
foreach ($result as $key => $value) 
  {

    $product_id='"'.$value['product_id'].'"';
    $created_at='"'.$value['created_at'].'"';
    $category_id='"'.$value['category_id'].'"';
    $category='"'.$value['category'].'"';
    $category_token='"'.$value['category_token'].'"';
    $image_url='"'.$value['image_url'].'"';
    $price='"'.$value['price'].'"';
    $special_price='"'.$value['special_price'].'"';
    $discount='"'.$value['discount'].'"';
    $tag='"'.$value['tag'].'"';
    $pack_size='"'.$value['pack_size'].'"';
    $color='"'.$value['color'].'"';
    $concern='"'.$value['concern'].'"';
    $spf='"'.$value['spf'].'"';
    $skin_type='"'.$value['skin_type'].'"';
    $preference='"'.$value['preference'].'"';
    $gender='"'.$value['gender'].'"';
    $formulation='"'.$value['formulation'].'"';
    $finish='"'.$value['finish'].'"';
    $occasion='"'.$value['occasion'].'"';
    $coverage='"'.$value['coverage'].'"';
    $fragrance='"'.$value['fragrance'].'"';
    $hair_type='"'.$value['hair_type'].'"';
    $skin_tone='"'.$value['skin_tone'].'"';
    $benefits='"'.$value['benefits'].'"';
    $bristle_type='"'.$value['bristle_type'].'"';
    $luxury_fragrance_type='"'.$value['luxury_fragrance_type'].'"';
    $age_range='"'.$value['age_range'].'"';
    $fragrance_type='"'.$value['fragrance_type'].'"';
    $collection='"'.$value['collection'].'"';
    $is_subscribable='"'.$value['is_subscribable'].'"';
    $main_image='"'.$value['main_image'].'"';
    $show_wishlist_button='"'.$value['show_wishlist_button'].'"';
    $button_text='"'.$value['button_text'].'"';
    $parent_category_id='"'.$value['parent_category_id'].'"';
    $brands_v1='"'.$value['brands_v1'].'"';
    $brands='"'.$value['brands'].'"';
    $image_url_utility='"'.$value['image_url_utility'].'"';
    $l4_category='"'.$value['l4_category'].'"';
    $featured_in_urls='"'.$value['featured_in_urls'].'"';
    $featured_in_title='"'.$value['featured_in_title'].'"';
    $attribute_set_name='"'.$value['attribute_set_name'].'"';
    $categorylevel1='"'.$value['categorylevel1'].'"';
    $catlevel1name='"'.$value['catlevel1name'].'"';
    $categorylevel2='"'.$value['categorylevel2'].'"';
    $catlevel2name='"'.$value['catlevel2name'].'"';
    $categorylevel3='"'.$value['categorylevel3'].'"';
    $catlevel3name='"'.$value['catlevel3name'].'"';
    $categorylevel4='"'.$value['categorylevel4'].'"';
    $catlevel4name='"'.$value['catlevel4name'].'"';
    $veg_nonveg='"'.$value['veg_nonveg'].'",';

    $ids[] =$product_id;

    $sd4_query_values[] = $product_id.','.$created_at.','.$category_id.','.$category.','.$category_token.','.$image_url.','.$price.','.$special_price.','.$discount.','.$tag.','.$pack_size.','.$color.','.$concern.','.$spf.','.$skin_type.','.$preference.','.$gender.','.$formulation.','.$finish.','.$occasion.','.$coverage.','.$fragrance.','.$hair_type.','.$skin_tone.','.$benefits.','.$bristle_type.','.$luxury_fragrance_type.','.$age_range.','.$fragrance_type.','.$collection.','.$is_subscribable.','.$main_image.','.$show_wishlist_button.','.$button_text.','.$parent_category_id.','.$brands_v1.','.$brands.','.$image_url_utility.','.$l4_category.','.$featured_in_urls.','.$featured_in_title.','.$attribute_set_name.','.$categorylevel1.','.$catlevel1name.','.$categorylevel2.','.$catlevel2name.','.$categorylevel3.','.$catlevel3name.','.$categorylevel4.','.$catlevel4name.','.$veg_nonveg.',';

}
    $ids1=implode(',',$ids);
    $i=0;
    $findids="select id from solr_dump_3 where product_id in (".$ids1.")";
    $command_findids = $readConnection->query($findids);
    $result_findids = $command_findids->fetchAll();
    foreach ($result_findids as $key => $value) 
    {
      $query = rtrim($sd4_query_values[$i],",");
      $sd4_update_query .= '('.$query.'),';
     
    
    if($i % 1000 == 0 || $i >= $all_products){

     $sd4_update_query1=rtrim($sd4_update_query,',');

    $sd4_query = "INSERT INTO solr_dump_4 (product_id,created_at,category_id,category,category_token,image_url,price,special_price,discount,tag,pack_size,color,concern,spf,skin_type,preference,gender,formulation,finish,occasion,coverage,fragrance,hair_type,skin_tone,benefits,bristle_type,luxury_fragrance_type,age_range,fragrance_type,collection,is_subscribable,main_image,show_wishlist_button,button_text,parent_category_id,brands_v1,brands,image_url_utility,l4_category,featured_in_urls,featured_in_titles,attribute_set_name,categorylevel1,catlevel1name,categorylevel2,catlevel2name,categorylevel3,catlevel3name,categorylevel4,catlevel4name,veg_nonveg) VALUES  ".$sd4_update_query1." ON DUPLICATE KEY UPDATE product_id =VALUES(product_id) ,created_at =VALUES(created_at) ,category_id =VALUES(category_id) ,category =VALUES(category) ,category_token =VALUES(category_token) ,image_url =VALUES(image_url) ,price =VALUES(price) ,special_price =VALUES(special_price) ,discount =VALUES(discount) ,tag =VALUES(tag) ,pack_size =VALUES(pack_size) ,color =VALUES(color) ,concern =VALUES(concern) ,spf =VALUES(spf) ,skin_type =VALUES(skin_type) ,preference =VALUES(preference) ,gender =VALUES(gender) ,formulation =VALUES(formulation) ,finish =VALUES(finish) ,occasion =VALUES(occasion) ,coverage =VALUES(coverage) ,fragrance =VALUES(fragrance) ,hair_type =VALUES(hair_type) ,skin_tone =VALUES(skin_tone) ,benefits =VALUES(benefits) ,bristle_type =VALUES(bristle_type) ,luxury_fragrance_type =VALUES(luxury_fragrance_type) ,age_range =VALUES(age_range) ,fragrance_type =VALUES(fragrance_type) ,collection =VALUES(collection) ,is_subscribable =VALUES(is_subscribable) ,main_image =VALUES(main_image) ,show_wishlist_button =VALUES(show_wishlist_button) ,button_text =VALUES(button_text) ,parent_category_id =VALUES(parent_category_id) ,brands_v1 =VALUES(brands_v1) ,brands =VALUES(brands) ,image_url_utility =VALUES(image_url_utility) ,l4_category =VALUES(l4_category) ,featured_in_urls =VALUES(featured_in_urls) ,featured_in_titles =VALUES(featured_in_titles) ,attribute_set_name =VALUES(attribute_set_name) ,categorylevel1 =VALUES(categorylevel1) ,catlevel1name =VALUES(catlevel1name) ,categorylevel2 =VALUES(categorylevel2) ,catlevel2name =VALUES(catlevel2name) ,categorylevel3 =VALUES(categorylevel3) ,catlevel3name =VALUES(catlevel3name) ,categorylevel4 =VALUES(categorylevel4) ,catlevel4name =VALUES(catlevel4name) ,veg_nonveg=VALUES(veg_nonveg)";

    $writeConnection->query($sd4_query);
    unset($sd4_update_query1);
    unset($sd4_update_query1);
  }
     $i++;
    }
  }else{
    echo "Products are not updated";
  }
}