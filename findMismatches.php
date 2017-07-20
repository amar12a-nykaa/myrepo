<?php
  
$GLUDO_API_HOST = "priceapi.nyk00-int.network";

function getNykaaConnection() {
  $nykaaConnection = new PDO("mysql:host=reports-read-replica.nyk00-int.network;dbname=nykaalive1", "anik", "slATy:2Rl9Me5mR");
  $nykaaConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
  return $nykaaConnection;
}

function getPWSConnection() {
  $pwsConnection = new PDO("mysql:host=priceapidb.nykaa-internal.com;dbname=nykaa", "api", "aU%v#sq1");
  $pwsConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
  return $pwsConnection;
}

function fetchNykaaProducts($offset, $limit) {
  global $enabledOnly, $nykaaConnection, $skuClause, $limitClause;

  $query = "SELECT e.sku, e.type_id, cpevn.value as name, cddisc.value as discount, 
      (CASE WHEN e.type_id = 'bundle' THEN
      (SELECT SUM(cpbs.selection_qty*cped.value) FROM catalog_product_bundle_selection AS cpbs
      LEFT JOIN catalog_product_entity_decimal AS cped
      ON cpbs.product_id = cped.entity_id AND cped.store_id = 0 AND cped.attribute_id = 60
      WHERE cpbs.parent_product_id = e.entity_id)
      ELSE cpedp.value END) AS price,
      (CASE WHEN e.type_id = 'bundle' THEN
      (cpedsp.value/100)*(SELECT SUM(cpbs.selection_qty*cped.value) FROM catalog_product_bundle_selection AS cpbs
      LEFT JOIN catalog_product_entity_decimal AS cped
      ON cpbs.product_id = cped.entity_id AND cped.store_id = 0 AND cped.attribute_id = 60
      WHERE cpbs.parent_product_id = e.entity_id)
      ELSE cpedsp.value END
      ) AS special_price,
      csi.qty, csi.is_in_stock, IF(csi.use_config_backorders=0 AND csi.backorders=1, '1','0') AS backorders

      FROM (select * from catalog_product_entity order by sku limit $offset, $limit)as e 
      LEFT JOIN catalog_product_entity_varchar cpevn on cpevn.entity_id = e.entity_id AND cpevn.attribute_id = 56 AND cpevn.store_id = 0 
      LEFT JOIN catalog_product_entity_decimal AS cpedp ON cpedp.entity_id = e.entity_id AND cpedp.attribute_id = 60 AND cpedp.store_id = 0
      LEFT JOIN catalog_product_entity_decimal AS cpedsp ON cpedsp.entity_id = e.entity_id AND cpedsp.attribute_id = 61 AND cpedsp.store_id = 0
      LEFT JOIN cataloginventory_stock_item AS csi ON csi.product_id = e.entity_id
      LEFT JOIN catalog_product_entity_int  AS cpeistl ON cpeistl.entity_id = e.entity_id AND cpeistl.attribute_id = 714 AND cpeistl.store_id = 0
      LEFT JOIN catalog_product_entity_int  AS cpeisd ON cpeisd.entity_id = e.entity_id AND cpeisd.attribute_id = 729 AND cpeisd.store_id = 0
      LEFT JOIN nykaalive1.catalog_product_entity_decimal cddisc ON cddisc.entity_id = e.entity_id AND cddisc.attribute_id=786

      WHERE $skuClause (cpeistl.value IS NULL OR cpeistl.value = 0) AND (cpeisd.value IS NULL OR cpeisd.value = 0) $limitClause";

  
  if($enabledOnly) {
    $query = "SELECT e.sku, e.type_id, cpevn.value as name, cddisc.value as discount,
      (CASE WHEN e.type_id = 'bundle' THEN
      (SELECT SUM(cpbs.selection_qty*cped.value) FROM catalog_product_bundle_selection AS cpbs
      LEFT JOIN catalog_product_entity_decimal AS cped
      ON cpbs.product_id = cped.entity_id AND cped.store_id = 0 AND cped.attribute_id = 60
      WHERE cpbs.parent_product_id = e.entity_id)
      ELSE cpedp.value END) AS price,
      (CASE WHEN e.type_id = 'bundle' THEN
      (cpedsp.value/100)*(SELECT SUM(cpbs.selection_qty*cped.value) FROM catalog_product_bundle_selection AS cpbs
      LEFT JOIN catalog_product_entity_decimal AS cped
      ON cpbs.product_id = cped.entity_id AND cped.store_id = 0 AND cped.attribute_id = 60
      WHERE cpbs.parent_product_id = e.entity_id)
      ELSE cpedsp.value END
      ) AS special_price,
      csi.qty, csi.is_in_stock, IF(csi.use_config_backorders=0 AND csi.backorders=1, '1','0') AS backorders

      FROM (select * from catalog_product_entity order by sku limit $offset, $limit)as e 
      INNER JOIN catalog_product_entity_int AS at_status_default ON ( at_status_default.entity_id = e.entity_id ) 
           AND (at_status_default.attribute_id = '80' ) AND at_status_default.store_id = 0
      LEFT JOIN catalog_product_entity_varchar cpevn on cpevn.entity_id = e.entity_id AND cpevn.attribute_id = 56 AND cpevn.store_id = 0 
      LEFT JOIN catalog_product_entity_decimal AS cpedp ON cpedp.entity_id = e.entity_id AND cpedp.attribute_id = 60 AND cpedp.store_id = 0
      LEFT JOIN catalog_product_entity_decimal AS cpedsp ON cpedsp.entity_id = e.entity_id AND cpedsp.attribute_id = 61 AND cpedsp.store_id = 0
      LEFT JOIN cataloginventory_stock_item AS csi ON csi.product_id = e.entity_id
      LEFT JOIN catalog_product_entity_int  AS cpeistl ON cpeistl.entity_id = e.entity_id AND cpeistl.attribute_id = 714 AND cpeistl.store_id = 0
      LEFT JOIN catalog_product_entity_int  AS cpeisd ON cpeisd.entity_id = e.entity_id AND cpeisd.attribute_id = 729 AND cpeisd.store_id = 0
      LEFT JOIN nykaalive1.catalog_product_entity_decimal cddisc ON cddisc.entity_id = e.entity_id AND cddisc.attribute_id=786

      WHERE $skuClause (cpeistl.value IS NULL OR cpeistl.value = 0) AND (cpeisd.value IS NULL OR cpeisd.value = 0) and at_status_default.value = 1 $limitClause";
  }
  #echo("Hello");
  #echo($query."\n\n");
  $stm = $nykaaConnection->prepare($query);
  $stm->execute();
  return $stm;
}



function processNykaaProduct($product) {
  global $skuClause;

  if(!empty($skuClause)) {
    print("==Magento Price from DB==\n");
    print_r($product);
  }

  // Remove unwanted fields from result
  unset($product['name']);

  $product['sku'] = trim($product['sku']);
  $product['price'] = (float)$product['price'];
  $product['special_price'] = ($product['special_price'] === NULL)? $product['price'] : $product['special_price'];
  $product['special_price'] = (float)$product['special_price'];
  $product['discount'] = (float)$product['discount'];
  $product['quantity'] = (int)$product['qty'];
  $product['is_in_stock'] = (int)$product['is_in_stock'];
  $product['backorders'] = (int)$product['backorders'];
  unset($product['qty']);

  if($product['type_id'] == 'bundle') {
    $product['price'] = round($product['price']);
    $product['special_price'] = round($product['special_price']);
    if($product['discount']) {
      $product['discount'] = 100 - $product['discount'];
    }
  }

  if(!empty($skuClause)) {
    print("==Magento Price==\n");
    print_r($product);
  }

  return $product;
}

function processPWSProduct($product) {
  global $skuClause;
  #print("product:");
  #print_r($product);

  if(empty($product)) return null;

  $product['mrp'] = (float)$product['mrp'];
  $product['discount'] = (float)$product['discount'];

  if(!isset($product['backorders'])) {
    $product['backorders'] = 0;
  }
  $product['backorders'] = (int)$product['backorders'];

  if(!empty($skuClause)) {
    print("==PWS Price==\n");
    print_r($product);
  }

  return $product;
}

function init() {
  global $counter, $sku, $skuClause, $enabledOnly, $sendMail, $limitClause;
  global $missingFile, $mismatchFile, $qtyMismatchFile;
  global $missingFilename, $mismatchFilename, $qtyMismatchFilename;
  global $nykaaConnection, $pwsConnection, $numMismatch, $numMissing;
  global $numAvailabilityMismatch, $numNegativeWithoutBackorders;

  $counter = 0;
  $numMissing = 0;
  $numMismatch = 0;
  $numAvailabilityMismatch = 0;
  $numNegativeWithoutBackorders = 0;

  $sku = '';
  $enabledOnly = True;
  $sendMail = False;

  $options = getopt("s:de");

  if(isset($options['s']) && !empty($options['s'])) {
    $sku = $options['s'];
  }

  $skuClause = '';
  if(!empty($sku)) {
    $skuClause = " e.sku='{$sku}' AND ";
  }

  if(isset($options['d'])) {
    $enabledOnly = False;
  }

  if(isset($options['e'])) {
    $sendMail = True;
  }

  $limit = 0;
  $limitClause = '';
  if($limit > 0) {
    $limitClause = "LIMIT $limit";
  }

  $missingFilename = '/tmp/missing_skus.csv';
  $mismatchFilename = '/tmp/price_mismatch_skus.csv';
  $qtyMismatchFilename = '/tmp/availability_mismatch_skus.csv';

  if(empty($skuClause)) {
    $missingFile = fopen($missingFilename, "w");
    $mismatchFile = fopen($mismatchFilename, "w");
    $qtyMismatchFile = fopen($qtyMismatchFilename, "w");
    fwrite($missingFile, "sku,type\n");
    fwrite($mismatchFile, "sku,type,magento price,pws price,magento sp,pws sp,magneto discount,pws discount\n");
    fwrite($qtyMismatchFile, "sku,type,magento quantity,pws quantity,magento backorders,pws backorders\n");
  }

  $nykaaConnection = getNykaaConnection();
}

function isGoodForMatching($product) {
  if($product['type_id'] == 'configurable') return false;
  if((float)$product['price'] < 1) return false;
  if(empty($product['sku'])) return false;
  if($product['type_id'] == 'bundle' && $product['discount'] == 0) return false;
  return true;
}

function fetchPWSProducts($products) {
  global $GLUDO_API_HOST;
  if(count($products) == 0) { return Array(); }
  $body = Array("products" => Array());
  foreach($products as $product)
  {
    $sku = strtoupper($product['sku']);
    array_push($body["products"], Array("sku"=>$sku, "type"=>$product['type_id']));
  }
#  echo("body:\n");
#  print_r($body);

	$ch = curl_init();
	curl_setopt($ch, CURLOPT_URL,"http://$GLUDO_API_HOST/apis/v1/pas.get");
	curl_setopt($ch, CURLOPT_POST, 1);
	curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($body));
	curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
	curl_setopt($ch, CURLOPT_HTTPHEADER, Array("Content-Type" => "application/json"));
	$response = json_decode(curl_exec($ch), True);
	curl_close($ch);

  if($response['status'] != "OK" && count($body['products']) > 0 || ! array_key_exists("skus", $response))
  {
    echo("[ERROR] Some problem in fetching data from Gludo APIs.");
    echo("\nresponse:\n");
    print_r($response);
    exit();
  }
  return array_values($response['skus']);
}

function logIfMissing($result1, $result2)
{
  global $numMissing, $skuClause, $missingFile;
  $skus1 = array_map(function ($a) { return $a['sku']; },$result1);
  $skus2 = array_map(function ($a) { return $a['sku']; },$result2);
  $missing = array_diff($skus1, $skus2);

  
  $arr1 = Array();
  foreach($result1 as $product)
  {
    $arr1[$product['sku']] = $product;
  }
#  echo("logIfMissing");
#  print_r($skus1);
#  print_r($skus2);
#  print_r($missing);

  foreach($missing as $sku)
  {
    $type = $arr1[$sku]['type_id'];
    $numMissing++;
    if(empty($skuClause)) {
      fwrite($missingFile, "$sku,$type\n");
    }
  }
}

function logIfMismatch($result1, $result2) {
  #print("logIfMismatch\n");
  global $numMismatch, $mismatchFile, $qtyMismatchFile, $skuClause;
  global $numAvailabilityMismatch, $numNegativeWithoutBackorders;

  $arr2 = Array();
  foreach($result2 as $product)
  {
    $arr2[$product['sku']] = $product;
  }
#  print("arr2:\n");
#  print_r($arr2);
  foreach($result1 as $prod)
  {
    $sku = $prod['sku'];
    $type = $prod['type_id'];
    if(array_key_exists($sku, $arr2))
    {
      $prod1 = $prod;
      $prod2 = $arr2[$sku];
#      print("prod1:\n");
#      print_r($prod1);
#      print("prod2:\n");
#      print_r($prod2);
      if(($prod1['price'] != $prod2['mrp']) || ($prod1['special_price'] != $prod2['sp'])) {
        $numMismatch++;
        //print("Mismatch found for SKU - $sku\n");
        if(empty($skuClause)) {
          fwrite($mismatchFile, "$sku,$type," . $prod1['price'] . "," . $prod2['mrp'] . "," .
            $prod1['special_price'] . "," . $prod2['sp'] . "," . $prod1['discount'] . "," .
            $prod2['discount'] . "\n");
        }
      }

      if($prod2['type'] !== 'bundle') {
        if(($prod1['quantity'] !== $prod2['quantity']) || 
           ($prod1['backorders'] !== $prod2['backorders'])) {
          if(($prod1['quantity'] < 0) && ($prod1['backorders'] == 0)) {
            $numNegativeWithoutBackorders++;
          }

          if(($prod1['quantity'] < 0) && ($prod1['backorders'] == 0) && 
             ($prod2['quantity'] == 0) && ($prod2['backorders'] == 0)) {
            //Ignore the case where magento quantity < 0 while pws quantity = 0
          } else {
            $numAvailabilityMismatch++;
            fwrite($qtyMismatchFile, "$sku,$type," . $prod1['quantity'] . "," . $prod2['quantity'] . "," .
                $prod1['backorders'] . "," . $prod2['backorders'] . "\n");
          }
        }
      }
    }
  }

}

function cleanup() {
  global $skuClause, $mismatchFile, $missingFile, $qtyMismatchFile;
  if(empty($skuClause)) {
    fclose($mismatchFile);
    fclose($missingFile);
    fclose($qtyMismatchFile);
  }
}

function sendReportEmail() {
  global $numMismatch, $numMissing, $numAvailabilityMismatch, $sendMail, $skuClause;
  global $counter, $missingFilename, $mismatchFilename, $qtyMismatchFilename;
  global $numNegativeWithoutBackorders;

  if(!$sendMail || !empty($skuClause)) return;

  $body = "\n";
  $body .= "Total products: $counter\n";
  $body .= "Total products missing: $numMissing\n";
  $body .= "Total price mismatches: $numMismatch\n";
  $body .= "Total availability mismatches: $numAvailabilityMismatch\n";
  //$body .= "Negative quantity products without backorders: $numNegativeWithoutBackorders\n";
  $body .= "\n\n";
  $body .= "Disclaimer: This is an automated email.\n";

  require 'PHPMailer/PHPMailerAutoload.php';

  $mail = new PHPMailer;
  $mail->isSMTP();
  $mail->SMTPDebug = 0;
  $mail->Host = "smtp.gmail.com";
  $mail->Port = 587;
  $mail->SMTPAuth = true;
  $mail->Username = "noreply@nykaa.com";
  $mail->Password = "minions_2015";
  $mail->setFrom('noreply@nykaa.com', 'No Reply');
  $mail->addAddress('sandeep@gludo.com', 'Sandeep Kadam');
  $mail->addAddress('kangkan@gludo.com', 'Kangkan Boro');
  $mail->addAddress('mayank@gludo.com', 'Mayank Jaiswal');
  $mail->addAddress('sanjay.suri@nykaa.com', 'Sanjay Suri');
  $mail->addAddress('gaurav.pandey@nykaa.com', 'Gaurav Pandey');
  $mail->addAddress('gaurav.pushkar@nykaa.com', 'Gaurav Pushkar');
  $mail->addAddress('gaurav.sharma@nykaa.com', 'Gaurav Sharma');
  $mail->addAddress('niharika.bajpai@nykaa.com', 'Niharika Bajpai');
  $mail->addAddress('rahil.khan@nykaa.com', 'Rahil Khan');
  $mail->addAddress('vijay.gupta@nykaa.com', 'Vijay Gupta');
  $mail->addAddress('anil.kumar@nykaa.com', 'Anil Kumar');
  $mail->addAddress('ashlesha.gawade@nykaa.com', 'Ashlesha Gawade');
  $mail->addAddress('oncall@nykaa.com', 'Oncall');
  $mail->addAddress('cataloging@nykaa.com', 'Cataloging');
  $mail->Subject = 'Price and availability mismatch report';
  $mail->Body = $body;

  if($numMismatch > 0) {
    $mail->addAttachment($mismatchFilename);
  }

  if($numMissing > 0) {
    $mail->addAttachment($missingFilename);
  }

  if($numAvailabilityMismatch > 0) {
    $mail->addAttachment($qtyMismatchFilename);
  }

  print("Sending report...\n");
  if (!$mail->send()) {
    echo "Mailer Error: " . $mail->ErrorInfo;
  } else {
    echo "Message sent!";
  }

  echo "\n";
}

#function logProducts($result1, $result2) {
#  global $skuClause;
#  if(!empty($skuClause)) {
#    print("==Magento Price==\n");
#    print_r($result1);
#    echo "\n";
#    print("==PWS Price==\n");
#    print_r($result2);
#    echo "\n";
#  }
#}

function printProgress() {
  global $counter, $numMismatch, $numMissing, $numAvailabilityMismatch, $numNegativeWithoutBackorders;
  if(++$counter%1000 == 0) {
    print('[' . date("D M j Y, G:i:s") . '] ');
    print("Progress: $counter, Price Mismatches: $numMismatch, Missing: $numMissing, Availability Mismatches: $numAvailabilityMismatch, Negative Without Backorders: $numNegativeWithoutBackorders\n");
  }
}

function printReport() {
  global $counter, $numMismatch, $numMissing, $numAvailabilityMismatch, $numNegativeWithoutBackorders;
  print("Total products: $counter\n");
  print("Total products missing: $numMissing\n");
  print("Total price mismatches: $numMismatch\n");
  print("Total availability mismatches: $numAvailabilityMismatch\n");
  print("Negative quantity products without backorders: $numNegativeWithoutBackorders\n");
}




$query = "select count(*) as count from catalog_product_entity ;";
#echo($query);
$stm = getNykaaConnection()->prepare($query);
$stm->execute();
$res = $stm->fetch(PDO::FETCH_ASSOC);
#print_r($res);
$nRows = $res['count'];
echo("Total products to be looped: ".$nRows. "\n");

init();
$offset = 0;
$limit = 10;
while(True)
{
  $stm = fetchNykaaProducts($offset, $limit);
  $offset += $limit;
  $counter += $limit;
#  if($offset > 500)
#  {
#    echo("Offset break\n");
#    break;
#  }
  $result1 = $stm->fetchall(PDO::FETCH_ASSOC);
  #echo("result1:");
  #print_r($result1);
  #$result1 = Array();

  $result1 = array_filter($result1, "isGoodForMatching");
  $result1 = array_map("processNykaaProduct", $result1);
#  echo("result1:");
#  print_r($result1);

  $result2 = fetchPWSProducts($result1);
  $result2 = array_map("processPWSProduct", $result2);
#  echo("\nresult2:\n");
#  print_r($result2);

  logIfMissing($result1, $result2);
  logIfMismatch($result1, $result2);

  printProgress();
  
  if($offset > $nRows)
  {
    echo("breaking ... ");
    break;
  }
  #echo("end of loop");
}

cleanup();
printReport();
sendReportEmail();

?>
