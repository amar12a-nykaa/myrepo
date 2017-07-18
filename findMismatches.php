<?php

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
  echo("Hello");
  echo($query."\n\n");
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

function shouldSkipMatching($product) {
  if($product['type_id'] == 'configurable') return true;
  if((float)$product['price'] < 1) return true;
  if(empty($product['sku'])) return true;
  if($product['type_id'] == 'bundle' && $product['discount'] == 0) return true;
  return false;
}

function fetchPWSProduct($products, $type) {
  $skus = Array();
  foreach($products as $product)
  {
    $product['sku'] = strtoupper($product['sku']);
    array_push($skus, $product['sku']);
  }
  #$sku = strtoupper($sku);
  $skus_str = join(',', $skus);
  print_r($skus_str);
  $body = Array();
  foreach($skus as $sku)
  {
    array_push($body, Array("sku"=>$sku, "type"=>$type));
  }
	$body = Array("products"=> $body);
  #print_r($body);
  $js = json_encode($body);
  echo("\n". $js. "\n\n");

  $host = "priceapi.nyk00-int.network";

	$ch = curl_init();

	curl_setopt($ch, CURLOPT_URL,"http://$host/apis/v1/pas.get");
	curl_setopt($ch, CURLOPT_POST, 1);
	curl_setopt($ch, CURLOPT_POSTFIELDS, $js);
	curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
	curl_setopt($ch, CURLOPT_HTTPHEADER, Array("Content-Type" => "application/json"));

	$response = curl_exec($ch);
	curl_close($ch);


#  $url = "http://$host/apis/v1/pas.get?sku=" . urlencode($skus_str) . "&type=$type";
#  $content = file_get_contents($url);
#  if($content === FALSE) {
#    print("Failed to fetch URL: $url. Retrying 1..\n");
#    $content = file_get_contents($url);
#    if($content === FALSE) {
#      print("Failed to fetch URL: $url. Retrying 2..\n");
#      $content = file_get_contents($url);
#      if($content === FALSE) {
#        print("Failed to fetch URL: $url. Skipping..\n");
#      }
#    }
#  }
	echo("\nresponse:\n");
	print_r($response);
	return json_decode($response, True);

  $data = json_encode($response);
	echo("\ndata:\n");
	print_r($data);
	exit();
  if(!isset($data['skus'][$sku])) return null;
  return $data['skus'][$sku];
}

#function logIfMissing($product, $sku, $type) {
function logIfMissing($result1_processed, $result2_processed)
{
  global $numMissing, $skuClause, $missingFile;

  if(empty($product)) {
    $numMissing++;
    if(empty($skuClause)) {
      fwrite($missingFile, "$sku,$type\n");
    }
    return true;
  }

  return false;
}

function logIfMismatch($result1, $result2, $sku, $type) {
  global $numMismatch, $mismatchFile, $qtyMismatchFile, $skuClause;
  global $numAvailabilityMismatch, $numNegativeWithoutBackorders;

  if(empty($result2)) return;

  if(($result1['price'] != $result2['mrp']) || ($result1['special_price'] != $result2['sp'])) {
    $numMismatch++;
    //print("Mismatch found for SKU - $sku\n");
    if(empty($skuClause)) {
      fwrite($mismatchFile, "$sku,$type," . $result1['price'] . "," . $result2['mrp'] . "," .
        $result1['special_price'] . "," . $result2['sp'] . "," . $result1['discount'] . "," .
        $result2['discount'] . "\n");
    }
  }

  if($result2['type'] !== 'bundle') {
    if(($result1['quantity'] !== $result2['quantity']) || 
       ($result1['backorders'] !== $result2['backorders'])) {
      if(($result1['quantity'] < 0) && ($result1['backorders'] == 0)) {
        $numNegativeWithoutBackorders++;
      }

      if(($result1['quantity'] < 0) && ($result1['backorders'] == 0) && 
         ($result2['quantity'] == 0) && ($result2['backorders'] == 0)) {
        //Ignore the case where magento quantity < 0 while pws quantity = 0
      } else {
        $numAvailabilityMismatch++;
        fwrite($qtyMismatchFile, "$sku,$type," . $result1['quantity'] . "," . $result2['quantity'] . "," .
            $result1['backorders'] . "," . $result2['backorders'] . "\n");
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

function logProducts($result1, $result2) {
  global $skuClause;
  if(!empty($skuClause)) {
    print("==Magento Price==\n");
    print_r($result1);
    echo "\n";
    print("==PWS Price==\n");
    print_r($result2);
    echo "\n";
  }
}

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
  #echo("offset: " .$offset."\n");
  $stm = fetchNykaaProducts($offset, $limit);
  $offset += 100;
  if($offset > 500)
  {
    echo("Offset break\n");
    break;
  }
  while($result1 = $stm->fetchall(PDO::FETCH_ASSOC)) {
    
    echo("result1:");
    print_r($result1);
    $result1_processed = Array();
    foreach( $result1 as $result)
    {
      if(shouldSkipMatching($result)) continue;
      $processed = processNykaaProduct($result);
      if(!array_key_exists($processed['type_id'], $result1_processed))
      {
        $result1_processed[$processed['type_id']] = Array();
      }
      array_push($result1_processed[$processed['type_id']], $processed);
    }
    echo("result1_processed:");
    print_r($result1_processed);
    #$sku = $result1['sku'];
    #$type = $result1['type_id'];

    foreach($result1_processed as $type => $products)
    {
      $result2 = fetchPWSProduct($products, $type);
      $result2_processed = Array();
      echo("\nresult2:\n");
      print_r($result2);
      foreach($result2['skus'] as $sku=>$result)
      {
        #$result2 = processPWSProduct($result2);
        $result2_processed[$sku] = processPWSProduct($result) ;
      }
    }
    print_r($result2_processed);
    logIfMissing($result1_processed, $result2_processed);
    logIfMismatch($result1_processed, $result2_processed);

    printProgress();
  }
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
