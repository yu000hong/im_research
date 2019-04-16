## GoBeleiveIO im_service源码阅读

### 设定目标

* 了解IM整体架构，包括im、imr、ims具体架构图
* im、imr、ims如何支持高可用，如何进行扩缩容
* 数据库、Redis在整个架构中的作用
* ims存储服务里，文件存储的具体格式

### 前提研究

* Github：https://github.com/GoBelieveIO/im_service
* 设计文档：https://github.com/GoBelieveIO/im_service/blob/master/doc/design.md
* API文档：https://github.com/GoBelieveIO/im_service/blob/master/doc/api.md

### 提出问题

* 客户端如何连接IM集群？通过NGINX代理、DNS轮询，或者客户端有一个IM服务器列表进行轮询？
* 为什么ims服务器实例只能倍增，而无法单独添加一个实例？
* imr和ims集群发生变化时，都需要im更新配置然后重新启动，有没有更优雅的方式保证IM服务可用的同时进行扩缩容？

**说明**

> 这里我把im_service里面的im、imr、ims拆分出来作为3个独立的项目im_debug、imr_debug、ims_debug。原项目里面包含了很多重复的代码，后面有时间希望把公共代码提出来作为一个独立的common项目。


