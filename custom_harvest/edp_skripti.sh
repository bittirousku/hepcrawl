
for filee in /home/henrikv/.virtualenvs/hepcrawl/src/hepcrawl/tmp/EDP/*.tar.gz
do
    echo $filee
    scrapy crawl EDP -a package_path=file:$filee
done