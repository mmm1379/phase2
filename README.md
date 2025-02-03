 # نصب
``` zsh
brew install kafka
```

 # اجرا

``` zsh
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
```

``` zsh
kafka-server-start /usr/local/etc/kafka/server.properties
```

سرور اصلی در Consumer.jar قرار دارد. برای پردازش، محتوای دریافت شده را به consumer.py تحویل میدهد و نتیجه اجرای مدل‌ها را از آن دریافت میکند. روی نتیجه، پنجره متحرکی قرار دارد و نتیجه تابع اعمال شده روی آن به producer.py بازگردانده میشود.

``` zsh
flink-1.20.0/bin/start-cluster.sh
flink-1.20.0/bin/flink run ./Consumer.jar
```

دلیل استفاده از pyflink، استفاده مستقیم از مدل‌های keras و h5 تولید شده توسط tensorflow بدون نیاز به تبدیل شدن آن به مدل قابل فهمی برای جاوا است.
``` zsh
python consumer.py
```


کاربر همان کد producer.py است که اطلاعات سنسور را فرستاده، نتیجه را به صورت normal یا alert دریافت میکند. 
``` zsh
python producer.py
```
# روند داده‌ها
کاربر(producer.py)، تصویر(ورودی از دوربین) و داده eeg به مدت ۳۰ ثانیه تولید میکند. این داده‌ها به سمت سرور(consumer.jar) فرستاده میشوند، سرور آنها را به consumer.py میفرستد تا به مدل ورودی داده شوند. پاسخ مدل‌ها به consumer.jar در قالب json بازگردانده میشود. consumer.jar نیز پس از اجرای تابعی روی آنها، پاسخ را به صورت string نتیجه(normal یا alert) به producer.py بازمیگرداند.
