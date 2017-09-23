<?php

if(!isset($_SERVER['HTTP_USER_AGENT'])){

  //$root_directory_path = dirname(dirname(dirname(__FILE__))) . DIRECTORY_SEPARATOR;
  echo $root_directory_path ="/var/www/nykaa/media/feed/";
//  require_once dirname(dirname(dirname(__FILE__))) . DIRECTORY_SEPARATOR . 'app' . DIRECTORY_SEPARATOR . 'Mage.php';
  require_once("../app/Mage.php");
    Mage::app();

  $feedPath = $root_directory_path;
  define('GOOGLE_PRODUCT_CATEGORY',19);
  define('ENTITY_ID',0);
  

  function process($arr)
  {
   /**/
    if($arr[ENTITY_ID] != "NULL")
    {
      $resource = Mage::getSingleton('core/resource');
      $readConnection = $resource->getConnection('core_read');
      $sku=$arr[ENTITY_ID];
      $readQue ="SELECT cpeicriteo.value FROM catalog_product_entity_varchar AS cpeicriteo ,catalog_product_entity AS cpe WHERE cpeicriteo.attribute_id=801 AND cpe.sku='".$sku."' AND cpe.entity_id = cpeicriteo.entity_id";
      $value=$readConnection->query($readQue);
      $row = $value->fetchAll();
      //$product =  Mage::getModel('catalog/product')->loadByAttribute('sku', $sku);
     // print_r($row);
   //   exit();
      if($row[0]['value']!=""){
        $arr[GOOGLE_PRODUCT_CATEGORY] = $row[0]['value'];
      }else{
        $arr[GOOGLE_PRODUCT_CATEGORY] = "";
      }
     /* $gpc=$product->getGoogleProductCategoryCriteo();
      if($gpc!=""){
        $arr[GOOGLE_PRODUCT_CATEGORY] = $gpc;
      }else{
        $arr[GOOGLE_PRODUCT_CATEGORY] = "";

      }*/
    }
   
    return $arr;
  }
  
  function curlcallsendmail($fields)
  {
    $postString=$fields;

        $ch = curl_init();
    $headers = array("Content-type: application/json;charset=\"utf-8\""); 
    curl_setopt($ch, CURLOPT_URL, "https://mandrillapp.com/api/1.0/messages/send.json");
    curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true );
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true );
    curl_setopt($ch, CURLOPT_HTTPHEADER,$headers);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $postString);

    $result = curl_exec($ch);
    print_r($result);
  }

  $countHead = true;
  $file = fopen($feedPath.'gmc_output.csv',"r");
  $fp = fopen($feedPath.'gmc_output_criteo.csv', 'w');


  $count=0;
  while(!feof($file))
  {
      $tempArr = fgetcsv($file);
      if(!$countHead){
        
        $arrVal = process($tempArr);
       // print_r( $arrVal);
        if(is_array($arrVal))
            fputcsv($fp, $arrVal);
      }
      else{
          $arrHeader = $tempArr;
          fputcsv($fp, $arrHeader);
          $countHead = false;
      }
      unset($tempArr);
      $count++;
  }
  fclose($file);
  fclose($fp);

  $linecount = count(file($feedPath.'gmc_output_criteo.csv'));
  
      $params = array(
        "key" => "GN_qEgEH9HcYuJxx0rHP7Q",
        "message" => array(
          "html" => "Hi <br> Criteo Total Count :".$linecount,
          "text" => "Hi <br> Criteo Total Count : ".$linecount,
          "to" => array(
              array("name" => "jyoti.kadam@nykaa.com", "email" => "jyoti.kadam@nykaa.com")
             // array("name" => "sayantan.ghosh@nykaa.com", "email" => "sayantan.ghosh@nykaa.com"),
              //array("name" => "hozefa.dhankot@nykaa.com", "email" => "hozefa.dhankot@nykaa.com"),
              //array("name" => "gaurav.pushkar@nykaa.com", "email" => "gaurav.pushkar@nykaa.com")
          ),
          "from_email" =>"noreply@nykaa.com",
          "from_name" => "noreply@nykaa.com",
          "subject" => "Criteo Count",
          "track_opens" => true,
          "track_clicks" => true
        ),
        "async" => false
    );

  $postString = json_encode($params);
    //print_r($postString);
  $arrVal =   curlcallsendmail($postString);
    
  
}

?>