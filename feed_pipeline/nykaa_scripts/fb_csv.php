  <?php
  include(dirname(__DIR__)."/app/Mage.php");
  Mage::app('admin');

  $read = $write = Mage::getSingleton('core/resource')->getConnection('core_read');
  $getProductsQuery = "select 
    sd4.product_id as id,
    sd3.sku as mpn,
    sd4.brands as brand,
    sd3.product_url as link,
    CONCAT(sd4.main_image,'?tr=h-600,w-600,q-100') as image_link,
    concat(sd4.price, ' INR') as price,
    concat(sd4.special_price, ' INR') as sale_price,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 710) as age_group,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 796) as gtin,
    sd3.name as title,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 778
        limit 1) as custom_label_0,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 779
        limit 1) as custom_label_1,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 780
        limit 1) as custom_label_2,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 781
        limit 1) as custom_label_3,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 782
        limit 1) as custom_label_4,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 711
        limit 1) as promotion_id,
    
            (case
                    when csi.is_in_stock = '1' then 'in stock'
                    else 'out of stock'
                end)
         as availability,
    (select 
            COALESCE((select 
                                cpet.value
                            from
                                catalog_product_entity_text as cpet
                            where
                                cpet.entity_id = sd3.product_id
                                    and cpet.attribute_id = 776
                            limit 1),
                        (select 
                                cpet.value
                            from
                                catalog_product_entity_text as cpet
                            where
                                cpet.entity_id = sd3.product_id
                                    and cpet.attribute_id = 57
                            limit 1))
        ) as description,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 775
        limit 1) as product_type,
    (select 
            cpev.value
        from
            catalog_product_entity_varchar as cpev
        where
            sd3.product_id = cpev.entity_id
                and cpev.attribute_id = 785
        limit 1) as google_product_category,
    concat('com.fsn.nykaa-android://nykaa/product?product_id=',
            sd3.product_id, '&is_native=1') as android_url,
    NULL as android_package,
    NULL as android_app_name,
    concat('Nykaa://product?product_id=', sd3.product_id) as ios_url,
    NULL as ios_app_store_id,
    NULL as ios_app_name,
    (select 
            eaov.value
        from
            eav_attribute_option_value as eaov
        where
            eaov.option_id in (select 
                    value
                from
                    catalog_product_entity_varchar as cpevc
                where
                    cpevc.attribute_id = 658
                        and cpevc.entity_id = sd3.product_id)
        limit 1) as color
from
    solr_dump_3 as sd3
        join
    solr_dump_4 as sd4 ON sd3.product_id = sd4.product_id
where
    sd3.product_id in (select 
            cpei.entity_id
        from
            catalog_product_entity_int as cpei
        where
            cpei.attribute_id = 718
                and cpei.value <> 1)
        and sd3.product_id in (select 
            cpei.entity_id
        from
            catalog_product_entity_int as cpei
        where
            attribute_id = 713 and value <> 1)
        and sd3.product_id in (select 
            cpei.entity_id
        from
            catalog_product_entity_int as cpei
        where
            attribute_id = 80 and value = 1)
        and sd4.price > 0";
  $result = $read->query($getProductsQuery);
  $productsData=$result->fetchAll();
  echo "query executed\n";
  // create final csv
  $row = 1;
  $res = array();

  $exclude = array();
  $excludeFlag = 0;
  // exclude these products
  if (($handle = fopen("/var/www/nykaa/media/feed/fb_exclude.csv", "r")) !== FALSE) {
      $excludeFlag = 1;
      while (($data = fgetcsv($handle, ",")) !== FALSE) {
          $exclude[] = $data[0];
      }
      fclose($handle);
  }
  else{
      echo "Cannot open file fb_exclude.csv\n";
      // exit;
  }

  $adult = array();
  $adultFlag = 0;
  // exclude these products
  if (($handle = fopen("/var/www/nykaa/media/feed/pids_lingerie.csv", "r")) !== FALSE) {
      $adultFlag = 1;
      while (($data = fgetcsv($handle, ",")) !== FALSE) {
          $adult[] = $data[0];
      }
      fclose($handle);
  }
  else{
      echo "Cannot open file pids_lingerie.csv\n";
      // exit;
  }

  // add flag for top_selling skus
  $topSkus = array();
  if (($handle = fopen("/var/www/nykaa/media/feed/top_1000_skus.csv", "r")) !== FALSE) {
      while (($data = fgetcsv($handle, ",")) !== FALSE) {
          $topSkus[] = $data[0];
      }
      fclose($handle);
  }
  else{
      echo "Cannot open file top_1000_skus.csv\n";
      // exit;
  }

  $count = 0;
  foreach($productsData as $data){
      $num = count($data);
      $count++;
      if($excludeFlag){
        if(in_array($data['id'], $exclude)){
          continue;
        }
      }

      if($adultFlag){
        if(in_array($data['id'], $adult) || $data['age_group']=="1" || $data['age_group']=="true"){
          $data['age_group'] = "adult";
        }
      }
      if($data['age_group']=="0" || $data['age_group']=="false"){
        $data['age_group'] = "";
      }
      // skip any unwanted line
      if($num < 24){
        continue;
      }

      // add parameters to link
      $data['link'] = $data['link']."?ptype=product&id=".$data['id'];

      // android links
      $data['android_package'] = "com.fsn.nykaa";
      $data['android_app_name'] = "Nykaa";

      // ios links
      $data['ios_app_name']="Nykaa - Beauty Shopping App";
      $data['ios_app_store_id']="1022363908";

      // top skus flag
      if(in_array($data['id'], $topSkus)){
        $data['custom_label_0'] = "1";
      }
      else {
        $data['custom_label_0'] = "0"; 
      }

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

      foreach ($data as $element=>$value) {
        if($value=="NULL"){
          $value = "";
        }
      }
      // add condition for every product as new
      $data['condition'] = 'new';
      $resfb[] = $data;
  }


  $pathToGenerateFb = "/var/www/nykaa/media/feed/fb_output_1.csv";  // fb csv
  $header=null;
  $createFileFb = fopen($pathToGenerateFb,"w");
  foreach ($resfb as $row) {
      if(!$header) {
          fputcsv($createFileFb,array_keys($row));
          fputcsv($createFileFb, $row);   // do the first row of data too
          $header = true; 
      }
      else {
          fputcsv($createFileFb, $row);
      }
  }

  fclose($createFileFb);


  // $feedPath = "/var/www/nykaa/media/feed/";
  // $count = count(file($feedPath.'fb_output_1.csv'));
  // if($linecount > 0){
  //   $msg = "Hi <br> fb feed generated successfully";
  // } else {
  //   $msg = "Hi <br> fb feed generated with issues";
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
  //     "subject" => "fb feed status",
  //     "track_opens" => true,
  //     "track_clicks" => true
  //     ),
  //     "async" => false
  // );

  // $postString = json_encode($params);
  // //print_r($postString);
  // $arrVal =     curlcallsendmail($postString);

  exit;
