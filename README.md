# dataengineering-savvy

You cannot raise your level as a #dataengineer if you say I haven't leveraged power of #scala & it's features. Many come from either traditional #ETL background or #java enterprise applications or plain java with #OOP #POJO. You may have learn various fancy automations at work but with scala, remember what you had learn school -
f(x) = do something with x or.. f(x,y) = x + y.
Wait, I'm not talking explicitly about #Algebra #Mathematics here; I'm talking with respect to apply #Engineering methods on #Data, take influence of #functionalprogramming & chain your #algorithm steps.

I will talk about scala's power with such examples in this repository. For example - Have you thought a try-catch block can be made as a wrapper? Ever since generics were introduced, & with the seasoning of scala's (any functional programming engine) higher-order functions this exciting dish is possible in hardly 8 lines of code; Say bye bye to spaghetti #code. Likewise there are plenty of #abstraction, #syntax sugars, expressions in this repo.

Few #USP of this library -
1. trySafely(unsafeCodeBlock = 1/0, errorMessage = "Cannot divide by zero", exceptionHandlingCodeBlock = -999)

How do I put this like formula / general expression wrt Functional Programming?
f ( unsafeCodeBlock = f(expression1), errorMessage = m, exceptionHandlingCodeBlock = f (expression2) )
Where expression1 & expression2 are compute codeblocks, m is an informal value for logging error.

It's like saying -
zaraSambhaalkeChalana(kyaChalaau = 1/0, muPeGiraToh = "chal hatt; tera chindi code bhatkela hai: Cannot divide by zero", malamPatti = -999) ;-)


2. spark.table("schema.table").backup("another_schema.table")
Surprised? Yes that is possible, but not like you are thinking now, it won't do INSERT INTO another_schema.table SELECT *FROM schema.table!! ;-)

3. How about -
val CST_TIME_ZONE = tz"US/CENTRAL"
val ISO_DATE = yearMonthDate"2022-04-15"
val ISO_DATE_TS = yearMonthDate24HrTs"2022-04-15 12:45:00"

4. spark.table("schema.some_partitioned_table").getAffectedPartitionDates(since = yearMonthDate24HrTs"2022-04-03 00:00:00")
