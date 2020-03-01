/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

package commons.model

/** class、object、case class、case object区别
  *
  * class 类似Java中的class；
  * object Scala不能定义静态成员，用定义单例对象代之；
  * case class被称为样例类，是一种特殊的类，常被用于模式匹配。
  *
  * 一、class 和 object 关系：
  * 1.单例对象不能带参数，类可以
  * 2.对象可以和类名一样时，object被称为伴生对象，class被称为伴生类；
  * 3.类和伴生对象可以相互访问其私有属性，但是它们必须在一个源文件当中；
  * 4.类只会被编译，不会被执行。要执行，必须在Object中。
  *
  * 二、case class 与 class 区别：
  * 1.初始化的时候可以不用new，也可以加上，但是普通类必须加new;
  * 2.默认实现了equals、hashCode方法；
  * 3.默认是可以序列化的，实现了Serializable；
  * 4.自动从scala.Product中继承一些函数;
  * 5.case class 构造函数参数是public的，我们可以直接访问；
  * 6.case class默认情况下不能修改属性值；
  * 7.case class最重要的功能，支持模式匹配，这也是定义case class的重要原因。
  *
  * 三、case class 和 case object 区别：
  * 1.类中有参和无参，当类有参数的时候，用case class ，当类没有参数的时候那么用case object。
  *
  * 四、当一个类被声名为case class的时候，scala会帮助我们做下面几件事情：
  * 1.构造器中的参数如果不被声明为var的话，它默认的话是val类型的，但一般不推荐将构造器中的参数声明为var
  * 2.自动创建伴生对象，同时在里面给我们实现子apply方法，使得我们在使用的时候可以不直接显示地new对象
  * 3.伴生对象中同样会帮我们实现unapply方法，从而可以将case class应用于模式匹配，关于unapply方法我们在后面的“提取器”那一节会重点讲解
  * 4.实现自己的toString、hashCode、copy、equals方法
  * 除此之此，case class与其它普通的scala类没有区别
  */

//***************** 输入表 *********************

/**
  * 用户访问动作表
  *
  * @param date               用户点击行为的日期
  * @param user_id            用户的ID
  * @param session_id         Session的ID
  * @param page_id            某个页面的ID
  * @param action_time        点击行为的时间点
  * @param search_keyword     用户搜索的关键词
  * @param click_category_id  某一个商品品类的ID
  * @param click_product_id   某一个商品的ID
  * @param order_category_ids 一次订单中所有品类的ID集合
  * @param order_product_ids  一次订单中所有商品的ID集合
  * @param pay_category_ids   一次支付中所有品类的ID集合
  * @param pay_product_ids    一次支付中所有商品的ID集合
  * @param city_id            城市ID
  */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long
                          )

/**
  * 用户信息表
  *
  * @param user_id      用户的ID
  * @param username     用户的名称
  * @param name         用户的名字
  * @param age          用户的年龄
  * @param professional 用户的职业
  * @param city         用户所在的城市
  * @param sex          用户的性别
  */
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    city: String,
                    sex: String
                   )

/**
  * 产品表
  *
  * @param product_id   商品的ID
  * @param product_name 商品的名称
  * @param extend_info  商品额外的信息
  */
case class ProductInfo(product_id: Long,
                       product_name: String,
                       extend_info: String
                      )

/**
  * 产品表
  *
  * @param product_id   商品的ID
  */
case class ProductInfo2(product_id: Long
                      )

/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 10/28/17 12:38 PM.
 * Author: wuyufei.
 */


case class CityClickProduct(city_id:Long,
                            click_product_id:Long)

case class CityAreaInfo(city_id:Long,
                        city_name:String,
                        area:String)

//***************** 输出表 *********************

/**
  *
  * @param taskid
  * @param area
  * @param areaLevel
  * @param productid
  * @param cityInfos
  * @param clickCount
  * @param productName
  * @param productStatus
  */
case class AreaTop3Product(taskid:String,
                           area:String,
                           areaLevel:String,
                           productid:Long,
                           cityInfos:String,
                           clickCount:Long,
                           productName:String,
                           productStatus:String)
