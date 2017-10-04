<?php
require_once("../../app/Mage.php");
ob_implicit_flush(1);
Mage::app();
$infos = array();
$file_name = "/var/www/html/nykaalive/release_dir/media/feed/master_output.csv";
$read = $write = Mage::getSingleton('core/resource')->getConnection('core_read');

if (($handle = fopen($file_name, "r")) !== FALSE) 
{
   $count = 0;
   $infos[] =array("id","parent_id","title","description","category","link","price","sale_price","brand","item_condition","availability","image","image2","image3","image4","sku","category_id","shade_name","shade_image","size","cat_id","d_sku","thumbnail_image","rating_summary","rating_count","configurable","offers","all_categories","created_at","is_parent","g_price","g_sale_price","g_url");

  	$path = '/var/www/html/nykaalive/release_dir/media/feed/nykaa-all-product-feed_new_1.csv';

    $fp = fopen($path, 'w');

  	@chmod($path, 0777);
	//fputcsv($fp,$infos,"\t");
    while (($data = fgetcsv($handle, 0, ",")) !== FALSE) 
    {
    	$count++;
   		if ($count == 1) { continue; }
        $shop_the_look=$data[21];
        if($shop_the_look ==1){
            continue;
        }
        $id=$data[0];
        $title =$data[4];
        if($data[6]!=""){
            $parent_id =$data[6];
        }else{
            $parent_id =$id;
        }
        $description =$data[8];
        $category_ids1 =explode("|", $data[71]);
        $category_ids=$category_ids1[0];
        $category1 = explode("|", $data[72]);
        $category=$category1[0];
        if($parent_id!="")
        {
            $query2="SELECT request_path FROM core_url_rewrite WHERE product_id= '".$parent_id."' AND store_id = 0 AND category_id IS NULL";
            //exit();
            $result2 = $read->query($query2);
            $parent_url1 = $result2->fetchAll();
            $parent_url = Mage::getBaseUrl().$parent_url1[0]['request_path'];

            $link = $parent_url."?ptype=product&pid=".$id;
        }else{
            $link =$data[43]."?ptype=product&pid=".$id;//add ptype and id
        }

        $link =$data[43];
        $image =$data[74];
        $price =$data[75]."INR";
        if($data[76] !=""){
            $sale_price = $data[76]."INR";
        }else{
            $sale_price = "";
        }
        $brands =  $data[42] ;
        $item_condition="new";
        if($data[51]==1)
        {
            $is_in_stock ="is_in_stock";
        }else{
            $is_in_stock ="out_of_stock";
        }
        $image2="";
        $image3="";
        $image4="";
        $sku=$data[1];
        $d_sku =$data[16];
        $thumbnail_image="";
        $rating_summary =$data[49];
        $rating_count =$data[47];
        
        $configurable = $data[2];
       
        $offer_id =$data[34];
        if($offer_id != NULL){
            $offers = count(explode("|", $offer_id));
        }else{
            $offers = 0;
        }

        $all_categories="";
        $created_at = $data[3];
        if($parent_id!=""){
            $is_parent="Yes";
        }else{
            $is_parent="No";
        }

        $g_price=$data[75];
        $g_sale_price=$data[76];
        $g_url=$data[43];
        if($configurable=='configurable')
        {
            $size="";
        }else
        {
            $size=$data[15];
        }
       
       //                    echo "\n".date("Y/m/d h:i:m:s");
        $query1="SELECT DISTINCT main_table.entity_id AS id,REPLACE(REPLACE( at_path.value, '.html', '') ,'/',' > ' ) AS path,main_table.parent_id FROM catalog_category_entity main_table LEFT JOIN catalog_category_entity_varchar at_path ON at_path.entity_id = main_table.entity_id  AND at_path.attribute_id = 47 WHERE  main_table.entity_id IN (SELECT category_id FROM   catalog_category_product WHERE  product_id = $id) AND main_table.level IN ( '3', '4' ) ";

      
        $result1 = $read->query($query1);

        $urls = $result1->fetchAll();
       // echo "\nafter result--".date("Y/m/d h:i:m:s");

        $pahtByName = '';
        
        if(count($urls) > 0)
        {
            foreach($urls as $cat)
            { 
                $pahtByName[] = $cat['path'];
            }
            //$pathArray[] = $pahtByName;
            $all_categories = implode(';', $pahtByName);
            unset($pathArray);
        }else
        {
            $all_categories ="";
        }
        $shade_name =$data[37];
        $shade_image =Mage::getBaseUrl().$data[37];
       // $availability =$data[113];
       // echo "\nafter forloop--".date("Y/m/d h:i:m:s");
//$all_categories = "";

        $infos[] = array($id,$parent_id,$title,$description,$category,$link,$price,$sale_price,$brands,$item_condition,$is_in_stock,$image,$image2,$image3,$image4,$sku,$category_ids,$shade_name,$shade_image,$size,$category_ids,$d_sku,$thumbnail_image,$rating_summary,$rating_count,$configurable,$offers,$all_categories,$created_at,$is_parent,$g_price,$g_sale_price,$g_url);
        
       
    }
    fclose($handle);
}

foreach ($infos as $info) 
{
 //   fputcsv($fp,$info,"\t");
    fputcsv ( $fp , $info , "," , '"' , '\'');
    /*flush();*/
}
 
fclose($fp);

?>