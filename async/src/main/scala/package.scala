package httpz

import com.ning.http.client.{Request => NingRequest, Response => NingResponse, _}
import scalaz.concurrent.{Future, Task}
import scala.collection.convert.decorateAsJava._
import java.util.Collections.singletonList
import java.util.{Collection => JCollection}
import scalaz._
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

package object async {

  implicit def toAsyncActionEOps[E, A](a: ActionE[E, A]) =
    new AsyncActionEOps(a)

  private def auth(user: String, password: String) = {
     import com.ning.http.client.Realm.{RealmBuilder,AuthScheme}
     new RealmBuilder()
       .setPrincipal(user)
       .setPassword(password)
       .setUsePreemptiveAuth(true)
       .setScheme(AuthScheme.BASIC)
       .build()
  }

  private def httpz2ning(r: Request): NingRequest = {
    val builder = new RequestBuilder
    builder
      .setUrl(r.url)
      .setHeaders(r.headers.mapValues(v => singletonList(v): JCollection[String]).asJava) // TODO
      .setQueryParams(r.params.mapValues(v => singletonList(v)).asJava)
      .setMethod(r.method)

    r.basicAuth.foreach{ case (user, pass) =>
      builder.setRealm(auth(user, pass))
    }

    r.body.foreach(builder.setBody)

    builder.build
  }

  private def execute(request: NingRequest): Promise[Throwable \/ Response[ByteArray]] = {
    val config = new AsyncHttpClientConfig.Builder()
      .setMaxConnections(1)
      .setIOThreadMultiplier(1)
    val client = new AsyncHttpClient(config.build)
    val promise = Promise[Throwable \/ Response[ByteArray]]
    val handler = new AsyncCompletionHandler[Unit] {
      def onCompleted(res: NingResponse) =
        try{
          import scala.collection.JavaConverters._
          val body = new ByteArray(res.getResponseBodyAsBytes)
          val status = res.getStatusCode
          val headers = mapAsScalaMapConverter(res.getHeaders).asScala.mapValues(_.asScala.toList)
          val r = \/-(Response(body, status, headers.toMap))
          client.closeAsynchronously()
          promise.success{
            r
          }
        }catch {
          case e: Throwable =>
            client.closeAsynchronously()
            promise.success(-\/(e))
        }
    }
    client.executeRequest(request, handler)
    promise
  }

  private[async] def request2async(r: Request): Task[Response[ByteArray]] = {
    val req = httpz2ning(r)
    val promise = execute(req)
    Task(Await.result(promise.future, 5.seconds)).flatMap{
      a => new Task(Future.now(a))
    }
  }

}
