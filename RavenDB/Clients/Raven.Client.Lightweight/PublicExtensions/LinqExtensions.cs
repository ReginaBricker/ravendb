//-----------------------------------------------------------------------
// <copyright file="LinqExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.Batches;
using Raven.Client.Linq;
using Raven.Abstractions.Extensions;
#if NETFX_CORE
using Raven.Client.WinRT.MissingFromWinRT;
#endif

namespace Raven.Client
{
	///<summary>
	/// Extensions to the linq syntax
	///</summary>
	public static partial class LinqExtensions
	{
		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
		/// <param name="source">The source for querying</param>
		/// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
		/// <returns></returns>
		public static IRavenQueryable<TResult> Include<TResult>(this IRavenQueryable<TResult> source, Expression<Func<TResult, object>> path)
		{
			source.Customize(x => x.Include(path));
			return source;
		}

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
		/// <typeparam name="TInclude">The type of the object that you want to include.</typeparam>
		/// <param name="source">The source for querying</param>
		/// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
		/// <returns></returns>
		public static IRavenQueryable<TResult> Include<TResult, TInclude>(this IRavenQueryable<TResult> source, Expression<Func<TResult, object>> path)
		{
			source.Customize(x => x.Include<TResult, TInclude>(path));
			return source;
		}

		/// <summary>
		/// Query the facets results for this query using aggregation
		/// </summary>
		public static DynamicAggregationQuery<T> AggregateBy<T>(this IQueryable<T> queryable, string path, string displayName= null)
		{
			return new DynamicAggregationQuery<T>(queryable, path, displayName);
		}

		/// <summary>
		/// Query the facets results for this query using aggregation
		/// </summary>
		public static DynamicAggregationQuery<T> AggregateBy<T>(this IQueryable<T> queryable, Expression<Func<T, object>> path)
		{
			return new DynamicAggregationQuery<T>(queryable, path);
		}

		/// <summary>
		/// Query the facets results for this query using aggregation with a specific display name
		/// </summary>
		public static DynamicAggregationQuery<T> AggregateBy<T>(this IQueryable<T> queryable, Expression<Func<T, object>> path, string displayName)
		{
			return new DynamicAggregationQuery<T>(queryable, path, displayName);
		}

#if !SILVERLIGHT && !NETFX_CORE
		/// <summary>
		/// Query the facets results for this query using the specified facet document with the given start and pageSize
		/// </summary>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public static FacetResults ToFacets<T>( this IQueryable<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null )
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			return ravenQueryInspector.GetFacets(facetSetupDoc, start, pageSize );
		}

        /// <summary>
        /// Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        public static FacetResults ToFacets<T>(this IQueryable<T> queryable, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var ravenQueryInspector = ((IRavenQueryInspector)queryable);

            return ravenQueryInspector.GetFacets(facetsList, start, pageSize);
        }

		/// <summary>
		/// Query the facets results for this query using the specified facet document with the given start and pageSize
		/// </summary>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public static FacetResults ToFacets<T>(this IDocumentQuery<T> query, string facetSetupDoc, int start = 0, int? pageSize = null)
		{
			var documentQuery = ((DocumentQuery<T>) query);
			return documentQuery.GetFacets(facetSetupDoc, start, pageSize);
		}

        /// <summary>
        /// Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        public static FacetResults ToFacets<T>(this IDocumentQuery<T> query, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var documentQuery = ((DocumentQuery<T>)query);

            return documentQuery.GetFacets(facetsList, start, pageSize);
        }
#endif

#if !SILVERLIGHT && !NETFX_CORE
		/// <summary>
		/// Lazily Query the facets results for this query using the specified facet document with the given start and pageSize
		/// </summary>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public static Lazy<FacetResults> ToFacetsLazy<T>( this IQueryable<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null )
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			var query = ravenQueryInspector.GetIndexQuery(isAsync: false);

			var lazyOperation = new LazyFacetsOperation( ravenQueryInspector.IndexQueried, facetSetupDoc, query, start, pageSize );

			var documentSession = ((DocumentSession)ravenQueryInspector.Session);
			return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
		}

        /// <summary>
        /// Lazily Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        public static Lazy<FacetResults> ToFacetsLazy<T>(this IQueryable<T> queryable, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            var query = ravenQueryInspector.GetIndexQuery(isAsync: false);

            var lazyOperation = new LazyFacetsOperation(ravenQueryInspector.IndexQueried, facetsList, query, start, pageSize);

            var documentSession = ((DocumentSession)ravenQueryInspector.Session);
            return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
        }

		/// <summary>
		/// Lazily Query the facets results for this query using the specified facet document with the given start and pageSize
		/// </summary>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public static Lazy<FacetResults> ToFacetsLazy<T>(this IDocumentQuery<T> query, string facetSetupDoc, int start = 0, int? pageSize = null)
		{
			var indexQuery = query.GetIndexQuery(isAsync: false);
			var documentQuery = ((DocumentQuery<T>)query);

			var lazyOperation = new LazyFacetsOperation(documentQuery.IndexQueried, facetSetupDoc, indexQuery, start, pageSize);

			var documentSession = ((DocumentSession)documentQuery.Session);
			return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
		}

        /// <summary>
        /// Lazily Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        public static Lazy<FacetResults> ToFacetsLazy<T>(this IDocumentQuery<T> query, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var indexQuery = query.GetIndexQuery(isAsync: false);
            var documentQuery = ((DocumentQuery<T>)query);

            var lazyOperation = new LazyFacetsOperation(documentQuery.IndexQueried, facetsList, indexQuery, start, pageSize);

            var documentSession = ((DocumentSession)documentQuery.Session);
            return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
        }
#endif

		/// <summary>
		/// Async Query the facets results for this query using the specified facet document with the given start and pageSize
		/// </summary>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public static Task<FacetResults> ToFacetsAsync<T>( this IQueryable<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null )
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			return ravenQueryInspector.GetFacetsAsync(facetSetupDoc, start, pageSize );
		}

		/// <summary>
		/// Async Query the facets results for this query using the specified list of facets with the given start and pageSize
		/// </summary>
		/// <param name="facets">List of facets</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public static Task<FacetResults> ToFacetsAsync<T>(this IQueryable<T> queryable, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
		{
			var facetsList = facets.ToList();

			if (!facetsList.Any())
				throw new ArgumentException("Facets must contain at least one entry", "facets");

			var ravenQueryInspector = ((IRavenQueryInspector)queryable);

			return ravenQueryInspector.GetFacetsAsync(facetsList, start, pageSize);
		}

		/// <summary>
		/// Async Query the facets results for this query using the specified facet document with the given start and pageSize
		/// </summary>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public static Task<FacetResults> ToFacetsAsync<T>(this IAsyncDocumentQuery<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null)
		{
			return queryable.GetFacetsAsync(facetSetupDoc, start, pageSize);
		}

		/// <summary>
		/// Project using a different type
		/// </summary>
		public static IQueryable<TResult> As<TResult>(this IQueryable queryable)
		{
			var ofType = queryable.OfType<TResult>();
			var results = queryable.Provider.CreateQuery<TResult>(ofType.Expression);
			return results;
		}

#if !NETFX_CORE
		/// <summary>
		/// Partition the query so we can intersect different parts of the query
		/// across different index entries.
		/// </summary>
		public static IRavenQueryable<T> Intersect<T>(this IQueryable<T> self)
		{
			var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
			Expression expression = self.Expression;
			if (expression.Type != typeof(IRavenQueryable<T>))
			{
				expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));
			}
			var queryable =
				self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof(T)), expression));
			return (IRavenQueryable<T>)queryable;
		}

		public static IRavenQueryable<TResult> ProjectFromIndexFieldsInto<TResult>(this IQueryable queryable)
		{
			return queryable.AsProjection<TResult>();
		}
#endif

		/// <summary>
		/// Project using a different type
		/// </summary>
		public static IRavenQueryable<TResult> AsProjection<TResult>(this IQueryable queryable)
		{
			var ofType = queryable.OfType<TResult>();
			var results = queryable.Provider.CreateQuery<TResult>(ofType.Expression);
			var ravenQueryInspector = ((RavenQueryInspector<TResult>)results);
			ravenQueryInspector.FieldsToFetch(typeof(TResult).GetProperties().Select(x => x.Name));
			return (IRavenQueryable<TResult>)results;
		}

#if !SILVERLIGHT && !NETFX_CORE

		/// <summary>
		/// Suggest alternative values for the queried term
		/// </summary>
		public static SuggestionQueryResult Suggest(this IQueryable queryable)
		{
			return Suggest(queryable, new SuggestionQuery());
		}

		/// <summary>
		/// Suggest alternative values for the queried term
		/// </summary>
		public static SuggestionQueryResult Suggest(this IQueryable queryable, SuggestionQuery query)
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query);
			return ravenQueryInspector.DatabaseCommands.Suggest(ravenQueryInspector.IndexQueried, query);
		}

		/// <summary>
		/// Lazy Suggest alternative values for the queried term
		/// </summary>
		public static Lazy<SuggestionQueryResult> SuggestLazy(this IQueryable queryable)
		{
			return SuggestLazy(queryable, new SuggestionQuery());
		}

		/// <summary>
		/// Lazy Suggest alternative values for the queried term
		/// </summary>
		public static Lazy<SuggestionQueryResult> SuggestLazy(this IQueryable queryable, SuggestionQuery query)
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query);

			var lazyOperation = new LazySuggestOperation(ravenQueryInspector.IndexQueried, query);

			var documentSession = ((DocumentSession)ravenQueryInspector.Session);
			return documentSession.AddLazyOperation<SuggestionQueryResult>(lazyOperation, null);
		}

#endif

		private static void SetSuggestionQueryFieldAndTerm(IRavenQueryInspector queryInspector, SuggestionQuery query, bool isAsync = false)
		{
			if (string.IsNullOrEmpty(query.Field) == false && string.IsNullOrEmpty(query.Term) == false)
				return;

			var lastEqualityTerm = queryInspector.GetLastEqualityTerm(isAsync);
			if (lastEqualityTerm.Key == null)
				throw new InvalidOperationException("Could not suggest on a query that doesn't have a single equality check");

			query.Field = lastEqualityTerm.Key;
			query.Term = lastEqualityTerm.Value;
		}

		/// <summary>
		/// Suggest alternative values for the queried term
		/// </summary>
		public static Task<SuggestionQueryResult> SuggestAsync(this IQueryable queryable, SuggestionQuery query)
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query, true);

			return ravenQueryInspector.AsyncDatabaseCommands.SuggestAsync(ravenQueryInspector.IndexQueried, query);
		}

		/// <summary>
		/// Suggest alternative values for the queried term
		/// </summary>
		public static Task<SuggestionQueryResult> SuggestAsync(this IQueryable queryable)
		{
			return SuggestAsync(queryable, new SuggestionQuery());
		}


		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// </summary>
		public static Lazy<IEnumerable<T>> Lazily<T>(this IQueryable<T> source)
		{
			return Lazily(source, null);
		}

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// As well as a function to execute when the value is evaluated
		/// </summary>
		public static Lazy<IEnumerable<T>> Lazily<T>(this IQueryable<T> source, Action<IEnumerable<T>> onEval)
		{
			var provider = source.Provider as IRavenQueryProvider;
			if (provider == null)
				throw new ArgumentException("You can only use Raven Queryable with Lazily");

			return provider.Lazily(source.Expression, onEval);
		}

		/// <summary>
		/// Returns a list of results for a query asynchronously. 
		/// </summary>
		public static async Task<IList<T>> ToListAsync<T>(this IQueryable<T> source)
		{
			var provider = source.Provider as IRavenQueryProvider;
			if (provider == null)
				throw new ArgumentException("You can only use Raven Queryable with ToListAsync");

			var documentQuery = provider.ToAsyncLuceneQuery<T>(source.Expression);
			provider.MoveAfterQueryExecuted(documentQuery);
			var result = await documentQuery.ToListAsync();
			return result.Item2;
		}

		/// <summary>
		/// Returns a first or default asynchronously. 
		/// </summary>
		public static async Task<T> FirstOrDefaultAsync<T>(this IQueryable<T> source)
		{
			var provider = source.Provider as IRavenQueryProvider;
			if (provider == null)
				throw new ArgumentException("You can only use Raven Queryable with FirstOrDefaultAsync");

			var documentQuery = provider
				.ToAsyncLuceneQuery<T>(source.Expression)
				.Take(1);
			provider.MoveAfterQueryExecuted(documentQuery);
			var result = await documentQuery.ToListAsync();
			return result.Item2.FirstOrDefault();
		}

		/// <summary>
		/// Returns whatever the query has any results asynchronously
		/// </summary>
		public static async Task<bool> AnyAsync<T>(this IQueryable<T> source)
		{
			var result = await source.CountAsync();
			return result > 0;
		}

		/// <summary>
		/// Returns the total count of results for a query asynchronously. 
		/// </summary>
		public static async Task<int> CountAsync<T>(this IQueryable<T> source)
		{
			var provider = source.Provider as IRavenQueryProvider;
			if (provider == null)
				throw new ArgumentException("You can only use Raven Queryable with CountAsync");

			var documentQuery = provider.ToAsyncLuceneQuery<T>(source.Expression)
			                            .Take(0);
			provider.MoveAfterQueryExecuted(documentQuery);
			var result = await documentQuery.ToListAsync();
			return result.Item1.TotalResults;
		}

#if !NETFX_CORE
		/// <summary>
		/// Perform a search for documents which fields that match the searchTerms.
		/// If there is more than a single term, each of them will be checked independently.
		/// </summary>
		public static IRavenQueryable<T> Search<T>(this IQueryable<T> self, Expression<Func<T, object>> fieldSelector, string searchTerms,
												   decimal boost = 1,
												   SearchOptions options = SearchOptions.Guess,
												   EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.EscapeAll)
		{
			var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
			Expression expression = self.Expression;
			if (expression.Type != typeof(IRavenQueryable<T>))
			{
				expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));
			}
			var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof(T)), expression,
																	  fieldSelector,
																	  Expression.Constant(searchTerms),
																	  Expression.Constant(boost),
																	  Expression.Constant(options),
																	  Expression.Constant(escapeQueryOptions)));
			return (IRavenQueryable<T>)queryable;
		}

		/// <summary>
		/// Perform an initial sort by lucene score.
		/// </summary>
		public static IOrderedQueryable<T> OrderByScore<T>(this IQueryable<T> self)
		{
			var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
			Expression expression = self.Expression;
			if (expression.Type != typeof(IRavenQueryable<T>))
			{
				expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));
			}
			var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof(T)), expression));
			return (IOrderedQueryable<T>)queryable;
		}
#endif

	}
}