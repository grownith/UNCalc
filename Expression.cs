using System;
using System.Threading;
using System.Diagnostics;
using System.Collections;
using System.Collections.Generic;

using NCalc.Domain;
using Antlr4.Runtime;
using System.IO;

namespace NCalc
{
	public class Expression
    {
        public EvaluateOptions Options { get; set; }

        /// <summary>
        /// Textual representation of the expression to evaluate.
        /// </summary>
        protected string OriginalExpression;

        public Expression(string expression) : this(expression, EvaluateOptions.None)
        {
        }

        public Expression(string expression, EvaluateOptions options)
        {
            if (String.IsNullOrEmpty(expression))
                throw new
                    ArgumentException("Expression can't be empty", "expression");

            OriginalExpression = expression;
            Options = options;
        }

        public Expression(LogicalExpression expression) : this(expression, EvaluateOptions.None)
        {
        }

        public Expression(LogicalExpression expression, EvaluateOptions options)
        {
            if (expression == null)
                throw new
                    ArgumentException("Expression can't be null", "expression");

            ParsedExpression = expression;
            Options = options;
        }

        #region Cache management
        private static bool _cacheEnabled = true;
        private static Dictionary<string, WeakReference> _compiledExpressions = new Dictionary<string, WeakReference>();
        private static readonly ReaderWriterLockSlim Rwl = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        public static bool CacheEnabled
        {
            get { return _cacheEnabled; }
            set
            {
                _cacheEnabled = value;

                if (!CacheEnabled)
                {
                    // Clears cache
                    _compiledExpressions = new Dictionary<string, WeakReference>();
                }
            }
        }

        /// <summary>
        /// Removed unused entries from cached compiled expression
        /// </summary>
        private static void CleanCache()
        {
            var keysToRemove = new List<string>();

            try
            {
                Rwl.EnterReadLock();
                foreach (var de in _compiledExpressions)
                {
                    if (!de.Value.IsAlive)
                    {
                        keysToRemove.Add(de.Key);
                    }
                }


                foreach (string key in keysToRemove)
                {
                    _compiledExpressions.Remove(key);
                    Debug.WriteLine("Cache entry released: " + key);
                }
            }
            finally
            {
                Rwl.ExitReadLock();
            }
        }

		#endregion
        public class EvaluationExceptionStrategy : DefaultErrorStrategy,IAntlrErrorListener<int>
		{
            /// <summary>
            /// Instead of recovering from exception
            /// <paramref name="e"/>
            /// , re-throw it wrapped
            /// in a
            /// <see cref="ParseCanceledException"/>
            /// so it is not caught by the
            /// rule function catches.  Use
            /// <see cref="System.Exception.InnerException()"/>
            /// to get the
            /// original
            /// <see cref="RecognitionException"/>
            /// .
            /// </summary>
            public override void Recover(Parser recognizer, RecognitionException e)
            {
                for (ParserRuleContext context = recognizer.Context; context != null; context = ((ParserRuleContext)context.Parent))
                {
                    context.exception = e;
                }
                throw new EvaluationException(e.Message);
            }

			/// <summary>
			/// Make sure we don't attempt to recover inline; if the parser
			/// successfully recovers, it won't throw an exception.
			/// </summary>
			/// <remarks>
			/// Make sure we don't attempt to recover inline; if the parser
			/// successfully recovers, it won't throw an exception.
			/// </remarks>
			/// <exception cref="Antlr4.Runtime.RecognitionException"/>
			public override IToken RecoverInline(Parser recognizer)
			{
				InputMismatchException e = new InputMismatchException(recognizer);
				for(ParserRuleContext context = recognizer.Context; context != null; context = ((ParserRuleContext)context.Parent))
				{
					context.exception = e;
				}
                
                if(recognizer is NCalcParser parser)
                {
                    if(parser.Errors == null)
                        parser.Errors = new List<string>();

                    parser.Errors.Add(e.Message);
                }

				throw e;
			}

			/// <summary>Make sure we don't attempt to recover from problems in subrules.</summary>
			/// <remarks>Make sure we don't attempt to recover from problems in subrules.</remarks>
			public override void Sync(Parser recognizer)
			{
			}

			public void SyntaxError(TextWriter output,IRecognizer recognizer,int offendingSymbol,int line,int charPositionInLine,string msg,RecognitionException e)
			{
                if(recognizer is NCalcParser parser)
                {
                    if(parser.Errors == null)
                        parser.Errors = new List<string>();

                    parser.Errors.Add(e.Message);
                }

				throw new EvaluationException(msg + "at " + line + ":" + charPositionInLine);
			}
		}

		public static LogicalExpression Compile(string expression, bool nocache)
        {
            LogicalExpression logicalExpression = null;

            if (_cacheEnabled && !nocache)
            {
                try
                {
                    Rwl.EnterReadLock();

                    if (_compiledExpressions.ContainsKey(expression))
                    {
                        Debug.WriteLine("Expression retrieved from cache: " + expression);
                        var wr = _compiledExpressions[expression];
                        logicalExpression = wr.Target as LogicalExpression;

                        if (wr.IsAlive && logicalExpression != null)
                        {
                            return logicalExpression;
                        }
                    }
                }
                finally
                {
                    Rwl.ExitReadLock();
                }
            }

            if (logicalExpression == null)
            {
                var strategy    = new EvaluationExceptionStrategy();
                var lexer = new NCalcLexer(Antlr4.Runtime.CharStreams.fromstring(expression));
                lexer.AddErrorListener(strategy);
                var parser = new NCalcParser(new CommonTokenStream(lexer)){ ErrorHandler = strategy };

                logicalExpression = parser.ncalcExpression().val;

                if (parser.Errors != null && parser.Errors.Count > 0)
                {
                    throw new EvaluationException(String.Join(Environment.NewLine, parser.Errors.ToArray()));
                }

                if (_cacheEnabled && !nocache)
                {
                    try
                    {
                        Rwl.EnterWriteLock();
                        _compiledExpressions[expression] = new WeakReference(logicalExpression);
                    }
                    finally
                    {
                        Rwl.ExitWriteLock();
                    }

                    CleanCache();

                    Debug.WriteLine("Expression added to cache: " + expression);
                }
            }

            return logicalExpression;
        }

        /// <summary>
        /// Pre-compiles the expression in order to check syntax errors.
        /// If errors are detected, the Error property contains the message.
        /// </summary>
        /// <returns>True if the expression syntax is correct, otherwiser False</returns>
        public bool HasErrors()
        {
            try
            {
                if (ParsedExpression == null)
                {
                    ParsedExpression = Compile(OriginalExpression, (Options & EvaluateOptions.NoCache) == EvaluateOptions.NoCache);
                }

                // In case HasErrors() is called multiple times for the same expression
                return ParsedExpression != null && Error != null;
            }
            catch(Exception e)
            {
                Error = e.Message;
                ErrorException = e;
                return true;
            }
        }

        public string Error { get; private set; }

        public Exception ErrorException { get; private set; }

        public LogicalExpression ParsedExpression { get; private set; }

        protected Dictionary<string, IEnumerator> ParameterEnumerators;
        protected Dictionary<string, object> ParametersBackup;

        public Func<TResult> ToLambda<TResult>()
        {
            if (HasErrors())
            {
                throw new EvaluationException(Error, ErrorException);
            }

            if (ParsedExpression == null)
            {
                ParsedExpression = Compile(OriginalExpression, (Options & EvaluateOptions.NoCache) == EvaluateOptions.NoCache);
            }

            var visitor = new LambdaExpressionVistor(Parameters, Options);
            ParsedExpression.Accept(visitor);

            var body = visitor.Result;
            if (body.Type != typeof(TResult))
            {
                body = System.Linq.Expressions.Expression.Convert(body, typeof(TResult));
            }

            var lambda = System.Linq.Expressions.Expression.Lambda<Func<TResult>>(body);
            return lambda.Compile();
        }

        public Func<TContext, TResult> ToLambda<TContext, TResult>() where TContext : class
        {
            if (HasErrors())
            {
                throw new EvaluationException(Error, ErrorException);
            }

            if (ParsedExpression == null)
            {
                ParsedExpression = Compile(OriginalExpression, (Options & EvaluateOptions.NoCache) == EvaluateOptions.NoCache);
            }

            var parameter = System.Linq.Expressions.Expression.Parameter(typeof(TContext), "ctx");
            var visitor = new LambdaExpressionVistor(parameter, Options);
            ParsedExpression.Accept(visitor);

            var body = visitor.Result;
            if (body.Type != typeof (TResult))
            {
                body = System.Linq.Expressions.Expression.Convert(body, typeof (TResult));
            }

            var lambda = System.Linq.Expressions.Expression.Lambda<Func<TContext, TResult>>(body, parameter);
            return lambda.Compile();
        }

        public T? Evaluate<T>() where T : struct,IConvertible => Evaluate<T>(out var value) ? value : (T?)null;
        public bool Evaluate<T>(out T val) where T : struct,IConvertible
        {
            var obj = Evaluate();
            if(obj == null || obj is string || ((obj is bool) ^ (typeof(T) == typeof(bool))))
            {
                val = default(T);
                return false;
            }

            try
            {
                val   = obj is T value ? value : (T)Convert.ChangeType(obj,typeof(T));
                return true;
            }
            catch
            {
                val = default(T);
                return false;
            }
        }

        public object Evaluate()
        {
            if (HasErrors())
            {
                throw new EvaluationException(Error, ErrorException);
            }

            if (ParsedExpression == null)
            {
                ParsedExpression = Compile(OriginalExpression, (Options & EvaluateOptions.NoCache) == EvaluateOptions.NoCache);
            }


            var visitor = new EvaluationVisitor(Options);
            visitor.EvaluateFunction += EvaluateFunction;
            visitor.EvaluateParameter += EvaluateParameter;
            visitor.Parameters = Parameters;

            // if array evaluation, execute the same expression multiple times
            if ((Options & EvaluateOptions.IterateParameters) == EvaluateOptions.IterateParameters)
            {
                int size = -1;
                ParametersBackup = new Dictionary<string, object>();
                foreach (string key in Parameters.Keys)
                {
                    ParametersBackup.Add(key, Parameters[key]);
                }

                ParameterEnumerators = new Dictionary<string, IEnumerator>();

                foreach (object parameter in Parameters.Values)
                {
                    if (parameter is IEnumerable)
                    {
                        int localsize = 0;
                        foreach (object o in (IEnumerable)parameter)
                        {
                            localsize++;
                        }

                        if (size == -1)
                        {
                            size = localsize;
                        }
                        else if (localsize != size)
                        {
                            throw new EvaluationException("When IterateParameters option is used, IEnumerable parameters must have the same number of items");
                        }
                    }
                }

                foreach (string key in Parameters.Keys)
                {
                    var parameter = Parameters[key] as IEnumerable;
                    if (parameter != null)
                    {
                        ParameterEnumerators.Add(key, parameter.GetEnumerator());
                    }
                }

                var results = new List<object>();
                for (int i = 0; i < size; i++)
                {
                    foreach (string key in ParameterEnumerators.Keys)
                    {
                        IEnumerator enumerator = ParameterEnumerators[key];
                        enumerator.MoveNext();
                        Parameters[key] = enumerator.Current;
                    }

                    ParsedExpression.Accept(visitor);
                    results.Add(visitor.Result);
                }

                return results;
            }

            ParsedExpression.Accept(visitor);
            return visitor.Result;

        }

        public event EvaluateFunctionHandler EvaluateFunction;
        public event EvaluateParameterHandler EvaluateParameter;

        private Dictionary<string, object> _parameters;

        public Dictionary<string, object> Parameters
        {
            get { return _parameters ?? (_parameters = new Dictionary<string, object>()); }
            set { _parameters = value; }
        }
    }
}
