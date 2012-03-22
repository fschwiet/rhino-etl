using System.Collections.Generic;
using Common.Logging.Configuration;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Tests.LoadTest
{
    using System.Data;
    using Core;
    using Xunit;
    using Rhino.Etl.Core.Operations;
    using System;

    /// <summary>
    /// This fixture is here to verify that we can handle large amount of data
    /// without consuming too much memory or crashing
    /// </summary>
    public class LoadTestFixture : BaseUserToPeopleTest
    {
        private const int expectedCount = 5000;
        private int currentUserCount;

        public LoadTestFixture()
        {
            currentUserCount = GetUserCount("1 = 1");
            using (PushDataToDatabase push = new PushDataToDatabase(expectedCount))
                push.Execute();
        }

        public void AssertUpdatedAllRows()
        {
            Assert.Equal(expectedCount + currentUserCount, GetUserCount("testMsg is not null"));

        }

        private static int GetUserCount(string where)
        {
            return Use.Transaction<int>("test", delegate(IDbCommand command)
            {
                command.CommandText = "select count(*) from users where " + where;
                return (int)command.ExecuteScalar();
            });
        }

        [Fact]
        public void CanUpdateAllUsersToUpperCase()
        {
            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new UpdateUserNames());
                update.Execute();
            }
            AssertUpdatedAllRows();
        }

        [Fact]
        public void CanBatchUpdateAllUsersToUpperCase()
        {
            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new BatchUpdateUserNames());
                update.Execute();
            }

            AssertUpdatedAllRows();
        }

        [Fact]
        public void BulkInsertUpdatedRows()
        {
            if(expectedCount != GetUserCount("1 = 1"))
                return;//ignoring test

            using (UpperCaseUserNames update = new UpperCaseUserNames())
            {
                update.RegisterLast(new BulkInsertUsers());
                update.Execute();
            }

            AssertUpdatedAllRows();
        }

        [Fact]
        public void CanCancelLoadGracefully()
        {
            using (var process = new ProcessCountingReadsBeforeCancel())
            {
                process.Execute();

                Assert.True(process.Count < expectedCount / 4);
            }
        }

        public class ProcessCountingReadsBeforeCancel : EtlProcess
        {
            public int Count;

            protected override void Initialize()
            {
                bool cancelled = false;
                var reader = new CancellableReader();
                Register(reader);
                Register(new TriggersAfter3(() => reader.Continue = false));
                Register(new ReportsCount(v => Count = v));
            }
        }

        public class CancellableReader : ReadUsers
        {
            public bool Continue = true;

            protected override bool ShouldContinue()
            {
                return Continue;
            }
        }

        public class TriggersAfter3 : AbstractOperation
        {
            readonly Action _action;
            public int Count = 0;

            public TriggersAfter3(Action action)
            {
                _action = action;
            }

            public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
            {
                foreach(var row in rows)
                {
                    Count++;
                    yield return row;
                    if (Count == 3)
                        _action();
                }
            }
        }

        public class ReportsCount : AbstractOperation
        {
            int _count;
            readonly Action<int> _listener;

            public ReportsCount(Action<int> listener)
            {
                _listener = listener;
            }

            public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
            {
                foreach(var row in rows)
                {
                    _listener(++_count);
                    yield return row;
                }
            }
        }
    }
}