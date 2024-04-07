import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

case class Name(firstName: String, lastName: String)
case class CustomerSpecs(customerId: Int, name: Name, email: String, contactNo: Int, address: String, age: Int, genderId: Int)
case class GenderSpecs(genderId: Int, gender: String)

case class Product(productId: Int, sizeId: Int, color: String)
case class ProductSpecs(product: Product, productName: String, brand: String, material: String, price: Double, quantity: Int, availabilityStatus: Int)
case class SizeSpecs(sizeId: Int, size: String)

case class OrderItems(productSpecs: ProductSpecs, quantity: Int, orderStatusId: Int)
case class OrderSpecs(orderId: Int, customer: CustomerSpecs, paymentId: Int, orderItems: Seq[OrderItems])
case class OrderStatusSpecs(orderStatusId: Int, orderStatus: String)

object FakeDataGenerator {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("FakeDataGenerator")
    val sc=new SparkContext(conf)

    // Customer
    val jsonPathCustomer = "customer_data.json"

    // Read the JSON file
    val jsonRDDCustomer: RDD[String] = sc.textFile(jsonPathCustomer)

    // Parse the JSON to a JValue
    val jValueRDDCustomer: RDD[JValue] = jsonRDDCustomer.map(parse(_))

    // Create an RDD[CustomerSpecs] from RDD[JValue] by extracting each field from the corresponding JValue
    val customerSpecsRDD: RDD[CustomerSpecs] = jValueRDDCustomer.map(jValue =>{
      implicit val formats = DefaultFormats

      val customerId = (jValue \ "customer_id").extract[Int]

      val name = (jValue \ "name")
      // Nested Fields
      val firstName = (name \ "first_name").extract[String]
      val lastName = (name \ "last_name").extract[String]

      val email = (jValue \ "email").extract[String]
      val contactNo = (jValue \ "contact_no").extract[Int]
      val address = (jValue \ "address").extract[String]
      val age = (jValue \ "age").extract[Int]
      val genderId = (jValue \ "gender_id").extract[Int]

      // Create a Customer Specs object
      CustomerSpecs(customerId,Name(firstName,lastName),email,contactNo,address,age,genderId)
    })

    // Gender
    val jsonPathGender = "gender_data.json"

    // Read the JSON file
    val jsonRDDGender: RDD[String] = sc.textFile(jsonPathGender)

    // Parse the JSON to a JValue
    val jValueRDDGender: RDD[JValue] = jsonRDDGender.map(parse(_))

    // Create an RDD[GenderSpecs] from RDD[JValue] by extracting each field from the corresponding JValue
    val genderSpecsRDD: RDD[GenderSpecs] = jValueRDDGender.map(jValue =>{
      implicit val formats = DefaultFormats

      val genderId = (jValue \ "gender_id").extract[Int]
      val gender = (jValue \ "gender").extract[String]

      // Create an Gender Specs object
      GenderSpecs(genderId, gender)
    })

    // Collect genderSpecsRDD to the driver as a Map
    val genderSpecsMap: Map[Int, String] = genderSpecsRDD.map(spec => (spec.genderId, spec.gender)).collect().toMap

    // Broadcast the genderSpecsMap to all nodes in the cluster
    val genderSpecsBroadcast: Broadcast[Map[Int, String]] = sc.broadcast(genderSpecsMap)

    // Now the genderSpecsBroadcast can be accessed in transformations
    // Mapping function to handle gender lookup
    def getGender(spec: CustomerSpecs, genderMap: Map[Int, String]): String = {
      val gender = genderMap.getOrElse(spec.genderId, "")
      if (gender != "") {
        gender // Return gender if found in the broadcasted map
      } else {
        "Unknown" // Return "Unknown" if genderId is -1
      }
    }

    // Map customerSpecsRDD with gender lookup function
   customerSpecsRDD.map(spec => (spec, getGender(spec, genderSpecsBroadcast.value))).foreach(println)



    // Product
    val jsonPathProduct = "product_data.json"

    // Read the JSON file
    val jsonRDDProduct: RDD[String] = sc.textFile(jsonPathProduct)

    // Parse the JSON to a JValue
    val jValueRDDProduct: RDD[JValue] = jsonRDDProduct.map(parse(_))

    // Create an RDD[ProductSpecs] from RDD[JValue] by extracting each field from the corresponding JValue
    val productSpecsRDD: RDD[ProductSpecs] = jValueRDDProduct.map(jValue =>{
      implicit val formats = DefaultFormats

      val product = (jValue \ "product")
      // Nested Fields
      val productId = (product \ "product_id").extract[Int]
      val size = (product \ "size_id").extract[Int]
      val color = (product \ "color").extract[String]

      val productName = (jValue \ "product_name").extract[String]
      val brand = (jValue \ "brand").extract[String]
      val material = (jValue \ "material").extract[String]
      val price = (jValue \ "price").extract[Double]
      val quantity = (jValue \ "quantity").extract[Int]
      val availabilityStatus = (jValue \ "availability_status").extract[Int]

      // Create a Product object
      ProductSpecs(Product(productId,size,color), productName, brand, material, price, quantity, availabilityStatus)
    })

    // Size
    val jsonPathSize = "size_data.json"

    // Read the JSON file
    val jsonRDDSize: RDD[String] = sc.textFile(jsonPathSize)

    // Parse the JSON to a JValue
    val jValueRDDSize: RDD[JValue] = jsonRDDSize.map(parse(_))

    // Create an RDD[SizeSpecs] from RDD[JValue] by extracting each field from the corresponding JValue
    val sizeSpecsRDD: RDD[SizeSpecs] = jValueRDDSize.map(jValue =>{
      implicit val formats = DefaultFormats

      val sizeId = (jValue \ "size_id").extract[Int]
      val size = (jValue \ "size").extract[String]

      // Create a Product object
      SizeSpecs(sizeId, size)
    })

    // Basic Analysis of products in inventory
    // 1. list all the products
    productSpecsRDD.foreach(println)

    // 2. No of products with color black
    productSpecsRDD.groupBy(_.product.color).map{
      case(x,y) => x->y.size
    }.filter(_._1=="Black").foreach(println)

    // 3. color with most products
    val res = productSpecsRDD.groupBy(_.product.color).map{
      case(x,y) => x->y.size
    }.sortBy(_._2).collect.last
    println(res)

    // 4. Availability status of a product with a specific id, color, and size
    productSpecsRDD.filter(x => x.product.productId==31 && x.product.sizeId==2 && x.product.color=="Red").map(x => x.availabilityStatus).foreach(println)


    // Collect sizeSpecsRDD to the driver as a Map
    val sizeSpecsMap: Map[Int, String] = sizeSpecsRDD.map(spec => (spec.sizeId, spec.size)).collect().toMap

    // Broadcast the sizeSpecsMap to all nodes in the cluster
    val sizeSpecsBroadcast: Broadcast[Map[Int, String]] = sc.broadcast(sizeSpecsMap)

    // Now the sizeSpecsBroadcast can be accessed in transformations
    // Mapping function to handle size lookup
    def getSize(spec: ProductSpecs, sizeMap: Map[Int, String]): String = {
      val size = sizeMap.getOrElse(spec.product.sizeId, "")
      if (size != "") {
        size // Return size if found in the broadcasted map
      } else if (spec.product.sizeId == -1) {
        "Unknown" // Return "Unknown" if sizeId is -1
      } else {
        "Invalid size" // Return "Invalid size" if size is not found and sizeId is not -1
      }
    }

    // Map productSpecsRDD with size lookup function
    // Mapping all the products with their sizes
    productSpecsRDD.map(spec => (spec, getSize(spec, sizeSpecsBroadcast.value))).foreach(println)
    // Mapping a single product with the required details and its size
    productSpecsRDD.filter(x => x.product.productId==72 && x.product.sizeId==1 && x.product.color=="Red").map(x => (x.productName,getSize(x, sizeSpecsBroadcast.value),x.product.color,x.brand,x.material,x.price,x.quantity)).foreach(println)



    // Order
    val jsonPathOrder = "order_data.json"

    // Read the JSON file
    val jsonRDDOrder: RDD[String] = sc.textFile(jsonPathOrder)

    // Parse the JSON to a JValue
    val jValueRDDOrder: RDD[JValue] = jsonRDDOrder.map(parse(_))

    // Create an RDD[OrderSpecs] from RDD[JValue] by extracting each field from the corresponding JValue
    val orderSpecsRDD: RDD[OrderSpecs] = jValueRDDOrder.map(jValue =>{
      implicit val formats = DefaultFormats

      val orderId = (jValue \ "order_id").extract[Int]

      val customer = (jValue \ "customer")
      // Nested Fields
      val customerId = (customer \ "customer_id").extract[Int]
      val name = (customer \ "name")
      // Nested Fields
      val firstName = (name \ "first_name").extract[String]
      val lastName = (name \ "last_name").extract[String]

      val email = (customer \ "email").extract[String]
      val contactNo = (customer \ "contact_no").extract[Int]
      val address = (customer \ "address").extract[String]
      val age = (customer \ "age").extract[Int]
      val genderId = (customer \ "gender_id").extract[Int]

      val paymentId = (jValue\ "payment_id").extract[Int]

      // Extract order_items as a JSON array
      val orderItemsArray = (jValue \ "order_items").children

      // Parse each JSON object in the array to create OrderItems objects
      val orderItems = orderItemsArray.map { item =>
        val productSpecs = ProductSpecs(
          Product(
            (item \ "product_specs" \ "product" \ "product_id").extract[Int],
            (item \ "product_specs" \ "product" \ "size_id").extract[Int],
            (item \ "product_specs" \ "product" \ "color").extract[String]
          ),
          (item \ "product_specs" \ "product_name").extract[String],
          (item \ "product_specs" \ "brand").extract[String],
          (item \ "product_specs" \ "material").extract[String],
          (item \ "product_specs" \ "price").extract[Double],
          (item \ "product_specs" \ "quantity").extract[Int],
          (item \ "product_specs" \ "availability_status").extract[Int]
        )
        val quantity = (item \ "quantity").extract[Int]
        val orderStatusId = (item \ "order_status_id").extract[Int]
        OrderItems(productSpecs, quantity, orderStatusId)
      }

      // Create an Order object
      OrderSpecs(orderId,CustomerSpecs(customerId,Name(firstName,lastName),email,contactNo,address,age,genderId),paymentId,orderItems)
    })

    // Order Status
    val jsonPathOrderStatus = "order_status_data.json"

    // Read the JSON file
    val jsonRDDOrderStatus: RDD[String] = sc.textFile(jsonPathOrderStatus)

    // Parse the JSON to a JValue
    val jValueRDDOrderStatus: RDD[JValue] = jsonRDDOrderStatus.map(parse(_))

    // // Create an RDD[OrderStatusSpecs] from RDD[JValue] by extracting each field from the corresponding JValue
    val orderStatusSpecsRDD: RDD[OrderStatusSpecs] = jValueRDDOrderStatus.map(jValue =>{
      implicit val formats = DefaultFormats

      // Extract the fields
      val orderStatusId = (jValue \ "order_status_id").extract[Int]
      val orderStatus = (jValue \ "order_status").extract[String]

      // Create an Order Status Specs object
      OrderStatusSpecs(orderStatusId, orderStatus)
    })

    // Collect orderStatusSpecsRDD to the driver as a Map
    val orderStatusSpecsMap: Map[Int, String] = orderStatusSpecsRDD.map(spec => (spec.orderStatusId, spec.orderStatus)).collect().toMap

    // Broadcast the orderStatusSpecsMap to all nodes in the cluster
    val orderStatusSpecsBroadcast: Broadcast[Map[Int, String]] = sc.broadcast(orderStatusSpecsMap)

    // Now the orderStatusSpecsBroadcast can be accessed in transformations
    // Mapping function to handle orderStatus lookup
    def getOrderStatus(spec: OrderSpecs, orderStatusMap: Map[Int, String]): Seq[String] = {
      val orderStatus = spec.orderItems.map(_.orderStatusId).map(statusId => orderStatusMap.getOrElse(statusId, ""))
      orderStatus.map{
        case status => status
        case "" => "Unknown"
      }
    }

    // Map orderSpecsRDD with orderStatus lookup function
    orderSpecsRDD.map(spec => (spec, getOrderStatus(spec, orderStatusSpecsBroadcast.value))).foreach(println)


    // Some useful insights that can be drawn out the RDDs(of type case class) created

    // 1. total corresponding to all the orders
    orderSpecsRDD.map(spec => spec.orderItems.map(orderItem => (orderItem.productSpecs.price * orderItem.quantity)).reduce(_ + _))

    // 2. total corresponding to a particular order
    orderSpecsRDD.filter(spec => spec.orderId==82).map(spec => spec.orderItems.map(orderItem => (orderItem.productSpecs.price * orderItem.quantity)).reduce(_ + _)).foreach(println)

    // 3. list of all the premium customers who have purchased for Rs. 500000 or above in total
    orderSpecsRDD.groupBy(_.customer.customerId).map{
      case (customerId, specs) => (customerId, specs.map(spec => spec.orderItems.map(orderItem => orderItem.productSpecs.price * orderItem.quantity).reduce(_ + _)).reduce(_ + _))
    }.filter(_._2 >= 500000)foreach(println)

    // 4. Find the retention rate of the e-commerce platform
    val noOfOrdersPlacedByACustomer = orderSpecsRDD.groupBy(_.customer.customerId).map{
      case (customerId, specs) => (customerId, specs.size)
    }

    noOfOrdersPlacedByACustomer.foreach(println)

    val totalNoOfCustomers = noOfOrdersPlacedByACustomer.count()

    val totalNoOfCustomersWithMoreThanOneOrder = noOfOrdersPlacedByACustomer.filter(_._2 > 1).count()

    println("Total No Of Customers: " + totalNoOfCustomers)
    println("Total No Of Customers With More Than One Order: " + totalNoOfCustomersWithMoreThanOneOrder)
    println("Retention Rate: " + (totalNoOfCustomersWithMoreThanOneOrder*100.0)/totalNoOfCustomers)

    // 5. Calculate sales by product to know which product is performing well and which is not
    val salesByProductRDD = orderSpecsRDD.flatMap(order =>
      order.orderItems.map(item =>
        ((item.productSpecs.product.productId, item.productSpecs.productName), item.quantity * item.productSpecs.price)
      )
    )

    val totalSalesByProductRDD = salesByProductRDD.reduceByKey(_ + _)

    totalSalesByProductRDD.collect().foreach{ case ((productId, productName), totalSales) =>
      println(s"Product ID: $productId, Product Name: $productName, Total Sales: $totalSales")
    }

    while (true) {
      println("Spark application is running. Sleeping for 60 seconds...")
      Thread.sleep(60000) // Sleep for 60 seconds (adjust as needed)
    }

    // Remember to unpersist the broadcast variable after use to release memory
    sizeSpecsBroadcast.unpersist()

    sc.stop()
  }
}