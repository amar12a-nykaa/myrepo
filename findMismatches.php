#!/usr/bin/php
<?php

function getNykaaConnection() {
  $nykaaConnection = new PDO("mysql:host=proddbnykaa-reports-06052017.ciel4c1bqlwh.ap-southeast-1.rds.amazonaws.com;dbname=nykaalive1", "anik", "slATy:2Rl9Me5mR");
  $nykaaConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
  return $nykaaConnection;
}

function getPWSConnection() {
  $pwsConnection = new PDO("mysql:host=ec2-52-220-91-218.ap-southeast-1.compute.amazonaws.com;dbname=nykaa", "api", "aU%v#sq1");
  $pwsConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
  return $pwsConnection;
}

function fetchNykaaProducts() {
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
      csi.qty

      FROM catalog_product_entity as e 
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
      csi.qty

      FROM catalog_product_entity as e 
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
  unset($product['qty']);

  $product['sku'] = trim($product['sku']);
  $product['price'] = (float)$product['price'];
  $product['special_price'] = ($product['special_price'] === NULL)? $product['price'] : $product['special_price'];
  $product['special_price'] = (float)$product['special_price'];
  $product['discount'] = (float)$product['discount'];

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

  if(!empty($skuClause)) {
    print("==PWS Price==\n");
    print_r($product);
  }

  return $product;
}

function init() {
  global $counter, $sku, $skuClause, $enabledOnly, $sendMail, $limitClause;
  global $missingFile, $mismatchFile, $missingFilename, $mismatchFilename;
  global $nykaaConnection, $pwsConnection, $numMismatch, $numMissing;

  $counter = 0;
  $numMissing = 0;
  $numMismatch = 0;

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
  $mismatchFilename = '/tmp/mismatch_skus.csv';

  if(empty($skuClause)) {
    $missingFile = fopen($missingFilename, "w");
    $mismatchFile = fopen($mismatchFilename, "w");
    fwrite($missingFile, "sku,type\n");
    fwrite($mismatchFile, "sku,type,magento price,pws price,magento sp,pws sp,magneto discount,pws discount\n");
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

/*
function fetchPWSProduct($sku) {
  global $pwsConnection;

  if(empty($pwsConnection)) {
    $pwsConnection = getPWSConnection();
  }

  $stm = $pwsConnection->prepare("SELECT sku, mrp, sp, discount FROM products WHERE sku = :sku");
  try {
    $stm->execute(array(':sku' => $sku));
  } catch (Exception $e) {
    $pwsConnection = getPWSConnection();
    $stm = $pwsConnection->prepare("SELECT sku, mrp, sp, discount FROM products WHERE sku = :sku");
    $stm->execute(array(':sku' => $sku));
  }

  return $stm->fetch(PDO::FETCH_ASSOC);
}
*/

function fetchPWSProduct($sku, $type) {
  $sku = strtoupper($sku);
  $host = "internal-SPSAPITargetGroup-internal-1197013483.ap-southeast-1.elb.amazonaws.com";
  $url = "http://$host/apis/v1/pas.get?sku=" . urlencode($sku) . "&type=$type";
  $data = json_decode(file_get_contents($url), true);
  if(!isset($data['skus'][$sku])) return null;
  return $data['skus'][$sku];
}

function logIfMissing($product, $sku, $type) {
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
  global $numMismatch, $mismatchFile, $skuClause;

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
}

function cleanup() {
  global $skuClause, $mismatchFile, $missingFile;
  if(empty($skuClause)) {
    fclose($mismatchFile);
    fclose($missingFile);
  }
}

function sendReportEmail() {
  global $numMismatch, $numMissing, $sendMail, $skuClause;
  global $counter, $missingFilename, $mismatchFilename;

  if(!$sendMail || !empty($skuClause)) return;

  $body = "\n";
  $body .= "Total products: $counter\n";
  $body .= "Total products missing: $numMissing\n";
  $body .= "Total price mismatches: $numMismatch\n";
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
  $mail->addAddress('niharika.bajpai@nykaa.com', 'Niharika Bajpai');
  $mail->addAddress('rahil.khan@nykaa.com', 'Rahil Khan');
  $mail->addAddress('vijay.gupta@nykaa.com', 'Vijay Gupta');
  $mail->Subject = 'Price mismatch report';
  $mail->Body = $body;

  if($numMismatch > 0) {
    $mail->addAttachment($mismatchFilename);
  }

  if($numMissing > 0) {
    $mail->addAttachment($missingFilename);
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
  global $counter, $numMismatch, $numMissing;
  if(++$counter%1000 == 0) {
    print('[' . date("D M j Y, G:i:s") . '] ');
    print("Progress: $counter, Mismatches: $numMismatch, Missing: $numMissing\n");
  }
}

function printReport() {
  global $counter, $numMismatch, $numMissing;
  print("Total products: $counter\n");
  print("Total products missing: $numMissing\n");
  print("Total price mismatches: $numMismatch\n");
}

init();

$stm = fetchNykaaProducts();
while($result1 = $stm->fetch(PDO::FETCH_ASSOC)) {
  if(shouldSkipMatching($result1)) continue;
  $result1 = processNykaaProduct($result1);

  $sku = $result1['sku'];
  $type = $result1['type_id'];

  $result2 = fetchPWSProduct($sku, $type);
  $result2 = processPWSProduct($result2);
  logIfMissing($result2, $sku, $type);
  logIfMismatch($result1, $result2, $sku, $type);

  printProgress();
}

cleanup();
printReport();
sendReportEmail();

?>
