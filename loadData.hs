import Text.Parsec (parse,anyChar)
import Text.Parsec.String --(Parser)
--import Text.Parsec.String.Parsec --(try)
import Text.Parsec.Char (oneOf, char, digit, satisfy,letter,spaces)
import Text.Parsec.Combinator (many1, choice, chainl1,option,eof,sepBy)
import Control.Applicative ((<|>), many,(<*) ,(*>),(<*>))
import Control.Monad --(void)
import Data.Char --(isLetter, isDigit)


data SimpleExpr = Num Integer
                 | Var String
                 | Add SimpleExpr SimpleExpr
                 | Parens SimpleExpr
                 | Str [Char]
                   deriving (Eq,Show)

usedChar = choice [letter,digit,(char '_'),(char '-')]

csvSep =  sepBy (many1 usedChar) (char ',')                 

loadTestData = do
	testFile <- readFile "data/test_rev2"
	let coloum = parse  csvSep "fail" $ head $ lines testFile 
	return coloum

test = parse csvSep "fail" "sfsdf,sdfsdf,sdfsdf,12312,234d"
