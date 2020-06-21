package com.abdulradi.bqschema

import cats.data.NonEmptyList
import eu.timepit.refined.types.string.NonEmptyString
import cats.kernel.Eq
import cats.Functor
import cats.Applicative
import cats.implicits._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.api.RefinedTypeOps
import com.abdulradi.bqschema.Recursion.SchemaF.Top

/**
  * This ADT represents Big Query Schema
  * It's isomorphic to com.google.cloud.Schema meaning no extra info or anythig missing
  * and it's possible to write a pair of total functions to convert back and forth between the two
  **/
sealed trait SchemaNode extends Product with Serializable
final case class Schema(fields: NonEmptyList[Schema.Field]) extends SchemaNode
object Schema {
  def of(head: Field, tail: Field*): Schema = 
    Schema(NonEmptyList.of(head, tail: _*))

  sealed trait Field extends SchemaNode {
    def info: Field.Info
  }
  object Field {
    final case class Primitive(info: Info, `type`: PrimitiveType) extends Field
    final case class Record(info: Info, fields: NonEmptyList[Field]) extends Field
    object Record {
      def of(info: Info, head: Field, tail: Field*): Record = 
        Record(info, NonEmptyList.of(head, tail: _*))
    }

    sealed abstract class PrimitiveType
    object PrimitiveType {  
      case object Bytes extends PrimitiveType
      case object String extends PrimitiveType
      case object Integer extends PrimitiveType
      case object Float extends PrimitiveType
      case object Numeric extends PrimitiveType
      case object Boolean extends PrimitiveType
      case object Timestamp extends PrimitiveType
      case object Date extends PrimitiveType
      case object Geography extends PrimitiveType
      case object Time extends PrimitiveType
      case object Datetime extends PrimitiveType
    }

    type Name = String Refined NonEmpty
    object Name extends RefinedTypeOps[Name, String]

    type Description = String Refined NonEmpty
    object Description extends RefinedTypeOps[Description, String]

    final case class Info(
      name: Name,
      mode: Mode,
      description: Option[Description]
    )

    sealed abstract class Mode
    object Mode {
      case object Required extends Mode
      case object Nullable extends Mode
      case object Repeated extends Mode
    }
  }

  implicit val eqSchemaInstance: Eq[Schema] = Eq.fromUniversalEquals
}

object Recursion {
  type Algebra[F[_], S] = F[S] => S
  type CoAlgebra[F[_], S] = S => F[S]
  // based on https://twitter.com/NicolasRinaudo/status/1273938027782508545
  def cata[F[_]: Functor, A, B](algebra: F[B] => B)(coAlgebra: A => F[A]): A => B = 
    new (A => B) { self =>
      override def apply(a: A): B = algebra(coAlgebra(a).fmap(self))
    }

  sealed trait SchemaF[+A]
  object SchemaF {
    final case class Top[+A](fields: NonEmptyList[A]) extends SchemaF[A]
    final case class Record[+A](info: Schema.Field.Info, fields: NonEmptyList[A]) extends SchemaF[A]
    final case class Primitive(info: Schema.Field.Info, `type`: Schema.Field.PrimitiveType) extends SchemaF[Nothing]

    implicit val functorInstance: Functor[SchemaF] = new Functor[SchemaF] {
      override def map[A, B](fa: SchemaF[A])(f: A => B): SchemaF[B] = fa match {
        case Top(fields) =>  Top(fields.map(f))
        case Record(info, fields) => Record(info, fields.map(f))
        case Primitive(info, typ) => Primitive(info, typ)
      }
    }
  }

  val projectSchema: SchemaNode => SchemaF[SchemaNode] = _ match {
    case Schema(fields) => SchemaF.Top(fields)
    case Schema.Field.Primitive(info, typ) => SchemaF.Primitive(info, typ)
    case Schema.Field.Record(info, fields) => SchemaF.Record(info, fields)
  }
  
  implicit class SchemaRecursionOps(val schema: Schema) extends AnyVal {
    def cata[A](algebra: Algebra[SchemaF, A]): A = Recursion.cata(algebra)(projectSchema).apply(schema)
  }
}

object Main extends App {
  import Schema.Field, Field._
  import eu.timepit.refined.auto._
  import Recursion._
  import io.circe.Json
  import io.circe.syntax._
  import io.circe.refined._

  def toJson(schema: Schema): Json = schema.cata[Json] {
    case SchemaF.Primitive(info, typ) =>
      Json.obj(
        "description" -> info.description.asJson,
        "name" -> info.name.asJson,
        "type" -> typ.toString.toUpperCase.asJson,
        "mode" -> info.mode.toString.toUpperCase.asJson
      )
    case SchemaF.Record(info, subfieldsAsJson) => 
      Json.obj(
        "description" -> info.description.asJson,
        "name" -> info.name.asJson,
        "type" -> "RECORD".asJson,
        "mode" -> info.mode.toString.toUpperCase.asJson,
        "fields" -> Json.arr(subfieldsAsJson.toList: _*)
      )
    case SchemaF.Top(fields) => 
      Json.arr(fields.toList: _*)
  }

  println(toJson(Schema.of(
    Primitive(Info("foo", Mode.Required, None), PrimitiveType.Boolean),
    Record.of(
      Info("bar", Mode.Nullable, Some("42")),
      Primitive(Info("baz", Mode.Repeated, None), PrimitiveType.Boolean),
    )
  )))
}