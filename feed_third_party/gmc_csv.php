<?php

include(dirname(__DIR__)."/app/Mage.php");
Mage::app('admin');

$read = $write = Mage::getSingleton('core/resource')->getConnection('core_read');
$getProductsQuery = "# get data from solr_dump_3 and solr_dump_4 if possible
  select sd3.sku as id, sd3.sku as mpn, sd4.brands as brand, sd3.product_url as link, sd4.main_image as image_link, sd4.price as price, 
  sd4.special_price as sale_price,
  # if value for adult attribute is not present, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 710) as adult,
  # if value for gtin  attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 796) as gtin, 
  # use google_title if present else use name for product
  (select COALESCE((select cpev.value from catalog_product_entity_varchar as cpev
                    where sd4.product_id = cpev.entity_id
            and cpev.attribute_id = 777),
                  sd3.name)) as title,
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 778 limit 1) as custom_label_0,
  # if value for custom_label_1 attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 779 limit 1) as custom_label_1,
  # if value for custom_label_2 attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 780 limit 1) as custom_label_2,
  # if value for custom_label_3 attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 781 limit 1) as custom_label_3,
  # if value for custom_label_4 attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 782 limit 1) as custom_label_4,
  # if value for custom_label_4 attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 711 limit 1) as promotion_id,
  # get stock status
  (select
  (case
    when sd3.is_in_stock = '1'
      then 'in stock'
    else 'out of stock'
  end)) as availability,
  (select COALESCE((select cpet.value 
          from catalog_product_entity_text as cpet
            where cpet.entity_id = sd4.product_id and cpet.attribute_id = 776 limit 1),
          (select cpet.value 
          from catalog_product_entity_text as cpet
            where cpet.entity_id = sd4.product_id and cpet.attribute_id = 57 limit 1))
  ) as description,
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 775 limit 1) as product_type,
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where sd4.product_id = cpev.entity_id
  and cpev.attribute_id = 785 limit 1) as google_product_category,
  concat('android-app://com.fsn.nykaa/com.fsn.nykaa-android/nykaa/product?product_id=',
  sd4.product_id, '&pid=googleadwords_int&c={campaignid}&is_retargeting=true')
  as mobile_android_app_link,
  sd4.product_id as product_id,
    from solr_dump_4 as sd4 join solr_dump_3 as sd3
  on sd3.product_id = sd4.product_id
  where sd4.product_id in (
  select cpei.entity_id from catalog_product_entity_int as cpei
  where cpei.attribute_id = 718 #dsku
    and cpei.value <> 1)
  and sd4.product_id in (
  select cpei.entity_id from catalog_product_entity_int as cpei
  where attribute_id = 713 #discontinued
    and value <> 1)
  and sd4.product_id in (
  select cpei.entity_id from catalog_product_entity_int as cpei
  where attribute_id = 80 #status
    and value = 1);";
$result = $read->query($getProductsQuery);
$productsData=$result->fetchAll();
// create final csv
$row = 1;
$res = array();
$brandNames = array();
// read data from changename.csv to append string to title
if (($handle = fopen("/var/www/nykaa/media/feed/changename.csv", "r")) !== FALSE) {
    while (($data = fgetcsv($handle, ",")) !== FALSE) {
        $brandNames[$data[0]] = $data[1];
    }
    fclose($handle);
}
else{
    echo "Cannot open file /var/www/nykaa/media/feed/changename.csv\n";
    // exit;

}
$exclude = array();
$excludeFlag = 0;
// exclude tese products
if (($handle = fopen("/var/www/nykaa/media/feed/google_exclude.csv", "r")) !== FALSE) {
    $excludeFlag = 1;
    while (($data = fgetcsv($handle, ",")) !== FALSE) {
        $exclude[] = $data[0];
    }
    fclose($handle);
}
else{
    echo "Cannot open file /var/www/nykaa/media/feed/google_exclude.csv\n";
    // exit;
}
$count = 0;
foreach($productsData as $data){
    $num = count($data);
    $count++;
    if($excludeFlag){
      if(in_array($data['product_id'], $exclude)){
        continue;
      }
    }

    // skip any unwanted line
    if($num < 22){
      continue;
    }

    // add parameters to link
    $data['link'] = $data['link']."?ptype=product&id=".$data['product_id'];

    //all custom_label_0 are to be set to NULL
    $data['custom_label_0'] = "";

    if(!($data['availability']=="out of stock" || $data['availability']=="in stock")){
        $data['availability'] = "out of stock";          
    }

    // if price is 0 then skip this product
    if(($data['price']=="0")){
      continue;
    }

    // if special price is 0 then make it blank
    if(($data['sale_price']=="0")){
      $data['sale_price'] = "";
    }

    // append data from changename.csv to title of respective products of brands
    if(array_key_exists($data['brand'], $brandNames)){
        $data['title'] = $data['title']." ".$brandNames[$data['brand']];
    }

    foreach ($data as $element) {
      if($element=="NULL")
        $element = "";
    }
    unset($data['product_id']);
    // add condition for every product as new
    $data['condition'] = 'new';    
    $res[] = $data;
}

$pathToGenerate = "/var/www/nykaa/media/feed/gmc_output.csv";  // your path and file name
$header=null;
$createFile = fopen($pathToGenerate,"w");
foreach ($res as $row) {
    if(!$header) {
        fputcsv($createFile,array_keys($row));
        fputcsv($createFile, $row);   // do the first row of data too
        $header = true; 
    }
    else {
        fputcsv($createFile, $row);
    }
}

$dskuQuery = "select cpe.sku as id, cpe.sku as mpn, 
(SELECT eaov.VALUE FROM catalog_product_entity_int  AS cpeib join eav_attribute_option_value AS eaov on eaov.option_id = cpeib.value
where cpe.entity_id = cpeib.entity_id AND eaov.store_id = 0 and cpeib.attribute_id = 732 limit 1) AS brand,
(select CONCAT('http://www.nykaa.com/',cpevpu.value) FROM catalog_product_entity_varchar as cpevpu where cpevpu.entity_id = cpe.entity_id AND cpevpu.attribute_id = 83 AND cpevpu.store_id = 0 limit 1) AS link,
(select CONCAT('http://www.nykaa.com/media/catalog/product',cpevimg.value) FROM catalog_product_entity_varchar  AS cpevimg where cpevimg.entity_id = cpe.entity_id and cpevimg.attribute_id = 70 limit 1) as image_link,
(select cpedp.value from catalog_product_entity_decimal AS cpedp where cpedp.entity_id = cpe.entity_id AND cpedp.attribute_id = 60 AND cpedp.store_id = 0 limit 1) AS price,
(select cpedsp.value from catalog_product_entity_decimal AS cpedsp where cpedsp.entity_id = cpe.entity_id AND cpedsp.attribute_id = 61 AND cpedsp.store_id = 0 limit 1) AS special_price,
(select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 710 limit 1) as adult,
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 796) as gtin,
(select COALESCE((select cpev.value from catalog_product_entity_varchar as cpev
                    where cpe.entity_id = cpev.entity_id
            and cpev.attribute_id = 777),
                  (select cpev.value from catalog_product_entity_varchar as cpev
                    where cpe.entity_id = cpev.entity_id
            and cpev.attribute_id = 56))) as title,
(select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 778 limit 1) as custom_label_0,
  # if value for custom_label_1 attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 779 limit 1) as custom_label_1,
  # if value for custom_label_2 attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 780 limit 1) as custom_label_2,
  # if value for custom_label_3 attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 781 limit 1) as custom_label_3,
  # if value for custom_label_4 attribute is not preset, put ' '
  (select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 782 limit 1) as custom_label_4,
(select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 711 limit 1) as promotion_id,
(select
  (case
    when csi.is_in_stock = '1'
      then 'in stock'
    else 'out of stock'
  end)
  from cataloginventory_stock_item as csi
    where cpe.entity_id = csi.product_id limit 1
  ) as availability,
(select COALESCE((select cpet.value 
          from catalog_product_entity_text as cpet
            where cpet.entity_id = cpe.entity_id and cpet.attribute_id = 776 limit 1),
          (select cpet.value 
          from catalog_product_entity_text as cpet
            where cpet.entity_id = cpe.entity_id and cpet.attribute_id = 57 limit 1))
  ) as description,
 (select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 775 limit 1) as product_type,
(select cpev.value
  from catalog_product_entity_varchar as cpev
     where cpe.entity_id = cpev.entity_id
  and cpev.attribute_id = 785 limit 1) as google_product_category,
concat('android-app://com.fsn.nykaa/com.fsn.nykaa-android/nykaa/product?product_id=',
  cpe.entity_id, '&pid=googleadwords_int&c={campaignid}&is_retargeting=true')
  as mobile_android_app_link,
cpe.entity_id as product_id
from catalog_product_entity as cpe  where cpe.entity_id in (
  select cpei.entity_id from catalog_product_entity_int as cpei
  where cpei.attribute_id = 718 #dsku
    and cpei.value = 1) 
and cpe.entity_id in ( select cpei.entity_id from catalog_product_entity_int as cpei
  where cpei.attribute_id = 80 #status
    and cpei.value = 1) 
and cpe.entity_id not in ( select cpei.entity_id from catalog_product_entity_int as cpei
  where cpei.attribute_id = 713 #discontinued
    and cpei.value = 1)
and cpe.entity_id not in ( select product_id from solr_dump_2 )
and cpe.type_id = 'simple';";

$result1 = $read->query($dskuQuery);
$productsData1=$result1->fetchAll();

foreach($productsData1 as $data){
    $num = count($data);

    if($excludeFlag){
      if(in_array($data['product_id'], $exclude)){
        continue;
      }
    }

    // skip any unwanted line
    if($num < 22){
      continue;
    }

    // add parameters to link
    $data['link'] = $data['link']."?ptype=product&id=".$data['product_id'];

    //all custom_label_0 are to be set to NULL
    $data['custom_label_0'] = "";

    if(!($data['availability']=="out of stock" || $data['availability']=="in stock")){
        $data['availability'] = "out of stock";          
    }

    // if price is 0 then skip this product
    if(($data['price']=="0")){
      continue;
    }

    // if special price is 0 then make it blank
    if(($data['sale_price']=="0")){
      $data['sale_price'] = "";
    }

    // append data from changename.csv to title of respective products of brands
    if(array_key_exists($data['brand'], $brandNames)){
        $data['title'] = $data['title']." ".$brandNames[$data['brand']];
    }

    foreach ($data as $element) {
      if($element=="NULL")
        $element = "";
    }
    unset($data['product_id']);
    // add condition for every product as new
    $data['condition'] = 'new';    
    $res1[] = $data;
}

foreach ($res1 as $row) {
    fputcsv($createFile, $row);
}
fclose($createFile);

// $feedPath = "/var/www/nykaa/media/feed/";
// $count = count(file($feedPath.'gmc_output.csv'));
// if($linecount > 0){
//   $msg = "Hi <br> gmc feed generated successfully";
// } else {
//   $msg = "Hi <br> gmc feed generated with issues";
// }
    
// $params = array(
//     "key" => "GN_qEgEH9HcYuJxx0rHP7Q",
//     "message" => array(
//     "html" => $msg,
//     "text" => $msg,
//     "to" => array(
//         array("name" => "jyoti.kadam@nykaa.com", "email" => "jyoti.kadam@nykaa.com"),
//         array("name" => "gaurav.pandey@nykaa.com", "email" => "gaurav.pandey@nykaa.com"),
//         array("name" => "hozefa.dhankot@nykaa.com", "email" => "hozefa.dhankot@nykaa.com"),
//         array("name" => "abhijeet.warhekar@nykaa.com", "email" => "abhijeet.warhekar@nykaa.com")
//     ),
//     "from_email" =>"noreply@nykaa.com",
//     "from_name" => "noreply@nykaa.com",
//     "subject" => "gmc feed status",
//     "track_opens" => true,
//     "track_clicks" => true
//     ),
//     "async" => false
// );

// $postString = json_encode($params);
// //print_r($postString);
// $arrVal =     curlcallsendmail($postString);

exit;
