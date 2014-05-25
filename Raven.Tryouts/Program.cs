using System;
using System.Collections.Generic;
using NLog;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Helpers;
using Raven.Tests.Issues;
using System;
using System.Linq;
using Raven.Tests.Spatial;
using Raven.Tryouts;
using SpatialIndexSamples;
using Xunit;

namespace Raven.Abstractions.Tests
{

    public class SpatialDoc
    {
        public string Id { get; set; }
        public string Shape { get; set; }
    }

    public class BBoxIndex : AbstractIndexCreationTask<SpatialDoc>
    {
        public BBoxIndex()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              doc.Shape
                          };

            Spatial(x => x.Shape, x => x.Cartesian.BoundingBoxIndex());
        }
    }

    public class QuadTreeIndex : AbstractIndexCreationTask<SpatialDoc>
    {
        public QuadTreeIndex()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              doc.Shape
                          };

          //  Spatial(x => x.Shape, x => x.Cartesian.QuadPrefixTreeIndex(6, new SpatialBounds(0, 0, 16, 16)));
            Spatial(x => x.Shape, x => x.Cartesian.QuadPrefixTreeIndex(6, new SpatialBounds(0, 0, 3, 3)));
        }
    }

    public class QuadTreeIndexGeographic : AbstractIndexCreationTask<SpatialDoc>
    {
        public QuadTreeIndexGeographic()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              doc.Shape
                          };

            Spatial(x => x.Shape, x => x.Geography.QuadPrefixTreeIndex(5));
        }
    }


    public class TestDoc
    {
        public int X { get; set; }

        public int Y { get; set; }
    }

    public class NumericIndex : AbstractIndexCreationTask<TestDoc>
    {
        public NumericIndex()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              X = doc.X / doc.Y
                          };


        }
    }


    internal class Program
    {


        private static void Main(string[] args)
        {
            //  var sample = new GeohashPrecisionSample();
            //   sample.Execute();
//            using (var store = new EmbeddableDocumentStore { RunInMemory = true })
//            {
//                store.Initialize();
//                new NumericIndex().Execute(store);
//
//                using (var session = store.OpenSession())
//                {
//                    for (int i = 0; i < 100; i++)
//                        session.Store(new TestDoc { X = i, Y = (i % 5 == 0) ? 0 : i });
//
//                    session.SaveChanges();
//                }
//
//                using (var session = store.OpenSession())
//                {
//                    var count = session.Query<TestDoc, NumericIndex>()
//                        .Customize(x => x.WaitForNonStaleResults())
//                        .Count();
//
//                    var stats = store.DatabaseCommands.GetStatistics();
//                }
//            }

//            var sample = new GeographicalVsCartesianSample();
//            sample.ExecuteIntersect();

//            // X XXX X
//            // X XXX X
//            // X XXX X
//            // X	 X
//            // XXXXXXX
//
            var polygon = "POLYGON ((0 0, 0 5, 1 5, 1 1, 5 1, 5 5, 6 5, 6 0, 0 0))";
            var simplePolygon = "POLYGON ((1 1, 1 4, 4 4, 4 1,1 1))";
            var simpleRectangle1 = "POLYGON((0 0 ,0 5 ,5 5, 5 0, 0 0))";
            var simpleRectangle2 = "POLYGON((1 2 ,2 3 ,3 3, 3 2, 1 2))";

            var rectangle1 = "2 2 4 4";
            var rectangle2 = "6 6 10 10";
            var rectangle3 = "0 0 6 6";
            var rectangle4 = "1 2 3 3";
//
            using (var store = new EmbeddableDocumentStore
            {
                RunInMemory = true,
                UseEmbeddedHttpServer = true
            })
            {
                store.Initialize();

                new BBoxIndex().Execute(store);
                new QuadTreeIndex().Execute(store);
                new QuadTreeIndexGeographic().Execute(store);
//
                using (var session = store.OpenSession())
                {
                    session.Store(new SpatialDoc {Shape = polygon});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SpatialDoc>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Count();
//
//
//                    var res1 = session.Query<SpatialDoc, BBoxIndex>()
//                                            .Spatial(x => x.Shape, x => x.Within(rectangle3)) ;

                    /*                   var result0 = session.Query<SpatialDoc, BBoxIndex>()
                                   .Customize(x => x.WaitForNonStaleResults())
                                   .Spatial(x => x.Shape, x => x.Contains(rectangle4)).ToList();
                    var result01 = session.Query<SpatialDoc, BBoxIndex>()
                                     .Customize(x => x.WaitForNonStaleResults())
                                     .Spatial(x => x.Shape, x => x.Contains(simpleRectangle2)).ToList();
 */

//                    var result2 = session.Query<SpatialDoc, BBoxIndex>()
//                                       .Customize(x => x.WaitForNonStaleResults())
//                                       .Spatial(x => x.Shape, x => x.Contains(simpleRectangle1)).ToList();
//
//                    var result21 = session.Query<SpatialDoc, QuadTreeIndex>()
//                    .Customize(x => x.WaitForNonStaleResults())
//                    .Spatial(x => x.Shape, x => x.Intersects(simpleRectangle1)).ToList();

                    //except
                    var result21 = session.Query<SpatialDoc, QuadTreeIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Spatial(x => x.Shape, x => x.Contains(simpleRectangle1)).ToList();

                }

//                using (var session = store.OpenSession())
//                {
//                    //var result = session.Query<SpatialDoc, BBoxIndex>()
//                    //    .Customize(x => x.WaitForNonStaleResults())
//                    //    .Spatial(x => x.Shape, x => x.Intersects(rectangle1))
//                    //    .Count();
//                    var result = session.Query<SpatialDoc, BBoxIndex>()
//                        .Customize(x => x.WaitForNonStaleResults())
//                        .Spatial(x => x.Shape, x => x.Intersects(rectangle1)).ToList();
//
//                    var quantity = result.Count;
//                    //
//                    //                    var result2 = session.Query<SpatialDoc, QuadTreeIndexGeographic>()
//                    //                                         .Spatial(x => x.Shape, x => x.Intersects(rectangle1))
//                    //                                         .Count();
//                    //
//                    //                    Assert.Equal(1, result);
//                    //                    Assert.Equal(1, result2);
//                }

                //    using (var session = store.OpenSession())
                //    {
                //        var result = session.Query<SpatialDoc, BBoxIndex>()
                //                            .Spatial(x => x.Shape, x => x.Intersects(rectangle2))
                //                            .Count();

                //        Assert.Equal(0, result);
                //    }

                //    using (var session = store.OpenSession())
                //    {
                //        var result = session.Query<SpatialDoc, BBoxIndex>()
                //                            .Spatial(x => x.Shape, x => x.Disjoint(rectangle2))
                //                            .Count();

                //        Assert.Equal(1, result);
                //    }

                //    using (var session = store.OpenSession())
                //    {
                //        var result = session.Query<SpatialDoc, BBoxIndex>()
                //                            .Spatial(x => x.Shape, x => x.Within(rectangle3))
                //                            .Count();

                //        Assert.Equal(1, result);
                //    }

                //    using (var session = store.OpenSession())
                //    {
                //        var result = session.Query<SpatialDoc, QuadTreeIndex>()
                //                            .Spatial(x => x.Shape, x => x.Intersects(rectangle2))
                //                            .Count();

                //        Assert.Equal(0, result);
                //    }

                //    using (var session = store.OpenSession())
                //    {
                //        var result = session.Query<SpatialDoc, QuadTreeIndex>()
                //                            .Spatial(x => x.Shape, x => x.Intersects(rectangle1))
                //                            .Count();

                //        Assert.Equal(0, result);
                //    }
                //}
            }
        }
    }
}
