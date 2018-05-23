package gajanans.spark.ActionSpark

class implicitTest {
  
	def main(args: Array[String]): Unit = {
			val oneStrTest = new ClassOne("test")
			val oneIntTest = new ClassOne(123)
			val oneIntTestD = new ClassOne(123.34)
			
//			oneStrTest.duplicatedString()
	}
}

class ClassOne[T](val input: T) { 

	implicit def toStrMethods(one: ClassOne[String]) = new ClassOneStr(one)
	implicit def toIntMethods(one: ClassOne[Int]) = new ClassOneInt(one)
	
}

class ClassOneStr(val one: ClassOne[String]) {
	def duplicatedString() = one.input + one.input
}

class ClassOneInt(val one: ClassOne[Int]) {
	def duplicatedInt() = one.input.toString + one.input.toString
}

