<?php
$url_base = 'https://u.nisemo.no/';
$upload_root = '<PLS SET THIS>';
function find_json_urls($folder, $prefix) {
    global $url_base;
    foreach(array_diff(scandir($folder), array('..', '.')) as $idx => $fn){
        if (substr_compare($fn, '.json', -5) === 0){
            echo $url_base.$prefix.$fn.PHP_EOL;
        } elseif (is_dir($fn)){
            find_json_urls($fn, $prefix.$fn.'/');
        }
    }
}
echo '<pre>';
find_json_urls($upload_root, '');
echo '</pre>';
?>
