import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.ScalaUDF

/**
  * Created by constantine on 12/06/2017.
  */
class WKT2Binary(override val f: AnyRef, override val dataType: DataType, override val inputTypes: Option[Seq[DataType]])
  extends UserDefinedFunction(f, dataType, inputTypes) {


//  override def apply = {
//    Column(ScalaUDF(f, dataType, exprs.map(_.expr), inputTypes.getOrElse(Nil)))
//  }
}
