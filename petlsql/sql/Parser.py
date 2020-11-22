#-------------------------------------------------------------------------
#Parser.py -- ATG file parser
#Compiler Generator Coco/R,
#Copyright (c) 1990, 2004 Hanspeter Moessenboeck, University of Linz
#extended by M. Loeberbauer & A. Woess, Univ. of Linz
#ported from Java to Python by Ronald Longo
#
#This program is free software; you can redistribute it and/or modify it
#under the terms of the GNU General Public License as published by the
#Free Software Foundation; either version 2, or (at your option) any
#later version.
#
#This program is distributed in the hope that it will be useful, but
#WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
#or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
#for more details.
#
#You should have received a copy of the GNU General Public License along
#with this program; if not, write to the Free Software Foundation, Inc.,
#59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#As an exception, it is allowed to write an extension of Coco/R that is
#used as a plugin in non-free software.
#
#If not otherwise stated, any source code generated by Coco/R (other than
#Coco/R itself) does not fall under the GNU General Public License.
#-------------------------------------------------------------------------*/


from . import ast
from .Scanner import *

import logging


class ErrorRec( object ):
    def __init__( self, l, c, s ):
        self.line   = l
        self.col    = c
        self.num    = 0
        self.str    = s


class Errors( object ):
    errMsgFormat = "file %(file)s : (%(line)d, %(col)d) %(text)s\n"
    #minErrDist   = 2
    #errDist      = minErrDist

    def __init__(self,fn):
        #eof          = False
        self.count    = 0         # number of errors detected
        self.fileName = fn

    def storeError(self, lvl, line, col, s ):
        print("%s[%d, %d]: %s" % (self.fileName, line, col, s))

    def SynErr(self, errNum, errPos=None ):
       line,col = errPos if errPos else self.getParsingPos( )
       self.storeError(logging.ERROR, line, col, self.errorMessages[ errNum ] )
       self.count += 1

    def SemErr(self, errMsg, errPos=None ):
       line,col = errPos if errPos else self.getParsingPos( )
       self.storeError(logging.ERROR, line, col, errMsg )
       self.count += 1

    def Warn(self, errMsg, errPos=None ):
       line,col = errPos if errPos else self.getParsingPos( )
       self.storeError(logging.WARNING, line, col, errMsg )


class Parser( object ):
   _EOF = 0
   _ident = 1
   _fixident = 2
   _paramId = 3
   _integer = 4
   _float = 5
   _string = 6
   maxT = 104

   T          = True
   x          = False
   minErrDist = 2

   
   def __init__( self, filename, errors_class=Errors):
      self.scanner     = None
      self.token       = None           # last recognized token
      self.la          = None           # lookahead token
      self.genScanner  = False
      self.tokenString = ''             # used in declarations of literal tokens
      self.noString    = '-none-'       # used in declarations of literal tokens
      self.errDist     = Parser.minErrDist
      self.macroMode = False
      errs = self.errors = errors_class(filename)
      errs.getParsingPos = self.getParsingPos
      errs.errorMessages = Parser.errorMessages

   def getParsingPos( self ):
      return self.la.line, self.la.col

   def SynErr( self, errNum ):
      if self.errDist >= Parser.minErrDist:
         self.errors.SynErr( errNum )

      self.errDist = 0

   def SemErr( self, msg ):
      if self.errDist >= Parser.minErrDist:
         self.errors.SemErr( msg )

      self.errDist = 0

   def Warning( self, msg ):
      if self.errDist >= Parser.minErrDist:
         self.errors.Warn( msg )

      self.errDist = 0

   def Successful( self ):
      return self.errors.count == 0;

   def LexString( self ):
      return self.token.val

   def LookAheadString( self ):
      return self.la.val

   def Get( self ):
      while True:
         self.token = self.la
         self.la = self.scanner.Scan( )
         if self.la.kind <= Parser.maxT:
            self.errDist += 1
            break
         
         self.la = self.token

   def Expect( self, n ):
      if self.la.kind == n:
         self.Get( )
      else:
         self.SynErr( n )

   def StartOf( self, s ):
      return self.set[s][self.la.kind]

   def ExpectWeak( self, n, follow ):
      if self.la.kind == n:
         self.Get( )
      else:
         self.SynErr( n )
         while not self.StartOf(follow):
            self.Get( )

   def WeakSeparator( self, n, syFol, repFol ):
      s = [ False for i in xrange( Parser.maxT+1 ) ]
      if self.la.kind == n:
         self.Get( )
         return True
      elif self.StartOf(repFol):
         return False
      else:
         for i in xrange( Parser.maxT ):
            s[i] = self.set[syFol][i] or self.set[repFol][i] or self.set[0][i]
         self.SynErr( n )
         while not s[self.la.kind]:
            self.Get( )
         return self.StartOf( syFol )

   def getCasesensitiveTokenValue(self,token):
        p = token.pos
        return str(self.scanner.buffer.getSlice(p, p+len(token.val)))

   def sql( self ):
      val, withc, names = None, None, None 
      self.context = None 
      while self.la.kind == 7:
         self.Get( )
         name = self.Name()
         if (self.la.kind == 9):
            names = self.columnNames()
         self.Expect(8)
         self.Expect(9)
         val = self.sqlselect()
         if names:
           val.set_header(names)
         if withc is None:
             withc = ast.WithContext()
         withc.addView(name, val)
         
         self.Expect(10)

      val = self.sqlselect()
      val.withcontext = withc
      self.result = val
      
      self.Expect(0)

   def Name( self ):
      id = self.Ident()
      while self.la.kind == 103:
         self.Get( )
         id1 = self.Ident()
         id = '%s.%s' % (id,id1) 

      id = ast.Identifier(id) 
      return id

   def columnNames( self ):
      names = [] 
      self.Expect(9)
      id = self.Ident()
      names.append(id) 
      while self.la.kind == 11:
         self.Get( )
         id = self.Ident()
         names.append(id) 

      self.Expect(10)
      return names

   def sqlselect( self ):
      self.Expect(12)
      oldcontext = self.context
      val = self.context = ast.SelectAst()
      val.parent = oldcontext
      
      if (self.la.kind == 19 or self.la.kind == 20):
         d = self.setQuantifier()
         val.distinct = d 
      if self.la.kind == 13:
         self.Get( )
         val.allColumns() 
      elif self.StartOf(1):
         self.selectList()
      else:
         self.SynErr(105)
      self.Expect(14)
      tbl = self.tableRefList()
      val.source = tbl 
      if (self.la.kind == 15):
         self.Get( )
         cond = self.searchCondition()
         val.set_where(cond) 
      if (self.la.kind == 16):
         self.Get( )
         self.Expect(17)
         self.groupList()
      if (self.la.kind == 18):
         self.Get( )
         self.Expect(17)
         self.orderList()
      self.context = oldcontext 
      return val

   def Ident( self ):
      if self.la.kind == 1:
         self.Get( )
         id = self.getCasesensitiveTokenValue(self.token)     
      elif self.la.kind == 2:
         self.Get( )
         id = self.getCasesensitiveTokenValue(self.token)[1:] 
      else:
         self.SynErr(106)
      return id

   def setQuantifier( self ):
      if self.la.kind == 19:
         self.Get( )
         distinct = True 
      elif self.la.kind == 20:
         self.Get( )
         distinct = False 
      else:
         self.SynErr(107)
      return distinct

   def selectList( self ):
      self.selectItem()
      while self.la.kind == 11:
         self.Get( )
         self.selectItem()


   def tableRefList( self ):
      joinType = ast.Join.INNER 
      val = self.tableReference()
      while self.StartOf(2):
         if self.StartOf(3):
            if (self.StartOf(4)):
               if self.la.kind == 28:
                  self.Get( )
               else:
                  if self.la.kind == 29:
                     self.Get( )
                     joinType = ast.Join.LEFT 
                  elif self.la.kind == 30:
                     self.Get( )
                     joinType = ast.Join.RIGHT 
                  elif self.la.kind == 31:
                     self.Get( )
                     if (self.la.kind == 32):
                        self.Get( )
                     joinType = ast.Join.FULL 
                  else:
                     self.SynErr(108)
            self.Get( )
            t = self.tableReference()
            join = self.joinSpecification()
            val = ast.JoinCursor(val,t,joinType, join) 
         else:
            self.Get( )
            t = self.tableReference()
            val = ast.JoinCursor(val, t, ast.Join.UNION) 

      return val

   def searchCondition( self ):
      vals = [] 
      val = self.boolTerm()
      while self.la.kind == 36:
         self.Get( )
         v = self.boolTerm()
         if not vals: vals.append(val)
         vals.append(v)
         

      if vals: val = ast.ConditionExpr('or', vals) 
      return val

   def groupList( self ):
      vars = self.nameList()
      self.context.set_group_by(vars) 

   def orderList( self ):
      asc = True 
      vars = self.nameList()
      if (self.la.kind == 62 or self.la.kind == 63):
         if self.la.kind == 62:
            self.Get( )
         else:
            self.Get( )
            asc = False 
      self.context.set_order_by(vars, asc) 

   def selectItem( self ):
      id,val = None,None 
      if self.StartOf(5):
         val = self.valueLitteral()
      elif self.StartOf(6):
         val = self.aggregateFunction()
      else:
         self.SynErr(109)
      if (self.la.kind == 8):
         self.Get( )
         id = self.Ident()
      self.context.addColumn(id, val) 

   def valueLitteral( self ):
      val = None 
      if self.la.kind == 1 or self.la.kind == 2:
         val = self.Name()
         if (self.la.kind == 9):
            self.Get( )
            args = self.procedureArgs()
            self.Expect(10)
            val = ast.Function(val, args) 
      elif self.StartOf(7):
         sign = 1 
         if (self.la.kind == 78 or self.la.kind == 79):
            if self.la.kind == 78:
               self.Get( )
            else:
               self.Get( )
               sign = -1 
         if self.la.kind == 4:
            val = self.Int()
         elif self.la.kind == 5:
            self.Get( )
            val = float(self.token.val) 
         else:
            self.SynErr(110)
         val = sign * val 
      elif self.la.kind == 6:
         val = self.String()
      elif self.la.kind == 47 or self.la.kind == 48:
         val = self.BoolLiteral()
      elif self.StartOf(8):
         val = self.standartFunction()
      elif self.la.kind == 80:
         self.Get( )
         val = self.caseExpr()
         self.Expect(81)
      elif self.la.kind == 9:
         self.Get( )
         if self.la.kind == 12:
            val = self.scalarSubquery()
         elif self.StartOf(5):
            val = self.valueExpr()
            val = ast.BracesExpr(val) 
         else:
            self.SynErr(111)
         self.Expect(10)
      elif self.la.kind == 3:
         val = self.SQLParameter()
      else:
         self.SynErr(112)
      return val

   def aggregateFunction( self ):
      d = False; f = ast.Aggregate.COUNT 
      if self.la.kind == 21:
         self.Get( )
         self.Expect(9)
         if self.la.kind == 13:
            self.Get( )
            val = None 
         elif self.StartOf(9):
            if (self.la.kind == 19 or self.la.kind == 20):
               d = self.setQuantifier()
            val = self.valueLitteral()
         else:
            self.SynErr(113)
         self.Expect(10)
      elif self.StartOf(10):
         if self.la.kind == 22:
            self.Get( )
            f = ast.Aggregate.AVG 
         elif self.la.kind == 23:
            self.Get( )
            f = ast.Aggregate.MAX 
         elif self.la.kind == 24:
            self.Get( )
            f = ast.Aggregate.MIN 
         else:
            self.Get( )
            f = ast.Aggregate.SUM 
         self.Expect(9)
         if (self.la.kind == 19 or self.la.kind == 20):
            d = self.setQuantifier()
         val = self.valueLitteral()
         self.Expect(10)
      elif self.la.kind == 26:
         self.Get( )
         f = ast.Aggregate.LIST 
         self.Expect(9)
         if (self.la.kind == 19 or self.la.kind == 20):
            d = self.setQuantifier()
         val = self.valueList()
         self.Expect(10)
      else:
         self.SynErr(114)
      val = ast.AggregateFunc(f, d, val) 
      if (self.la.kind == 27):
         cond = self.filterClause()
         val.selector = cond 
      return val

   def valueList( self ):
      val = self.valueLitteral()
      while self.la.kind == 11:
         self.Get( )
         v = self.valueLitteral()
         if not isinstance(val, list):
             val = [val]
         val.append(v)
         

      return val

   def filterClause( self ):
      self.Expect(27)
      self.Expect(9)
      self.Expect(15)
      val = self.searchCondition()
      self.Expect(10)
      return val

   def tableReference( self ):
      id, tblname = '', '' 
      if self.la.kind == 1 or self.la.kind == 2:
         tblname = self.Name()
      elif self.la.kind == 6:
         tblname = self.String()
      else:
         self.SynErr(115)
      if (self.la.kind == 8):
         self.Get( )
         id = self.Ident()
      tbl = ast.Table(tblname, id) 
      return tbl

   def joinSpecification( self ):
      val =None 
      if self.la.kind == 34:
         self.Get( )
         cond = self.searchCondition()
         val = ast.JoinCondition(cond) 
      elif self.la.kind == 35:
         self.Get( )
         self.Expect(9)
         columns = self.NameList()
         self.Expect(10)
         val = ast.JoinUsing(columns) 
      else:
         self.SynErr(116)
      return val

   def NameList( self ):
      id = self.Name()
      val = [id] 
      while self.la.kind == 11:
         self.Get( )
         id = self.Name()
         val.append(id) 

      return val

   def String( self ):
      self.Expect(6)
      val = self.getCasesensitiveTokenValue(self.token)[1:-1] 
      return val

   def boolTerm( self ):
      vals = [] 
      val = self.boolFactor()
      while self.la.kind == 37:
         self.Get( )
         v = self.boolFactor()
         if not vals: vals.append(val)
         vals.append(v)
         

      if vals: val = ast.ConditionExpr('and',vals) 
      return val

   def boolFactor( self ):
      neg = False 
      if (self.la.kind == 38):
         self.Get( )
         neg = True 
      val = self.primaryCondition()
      if neg: val = ast.Negation(val) 
      return val

   def primaryCondition( self ):
      val = self.valueLitteral()
      if (self.StartOf(11)):
         val = self.compareOperand(val)
      return val

   def compareOperand( self, arg ):
      is_true = True 
      if self.StartOf(12):
         if (self.la.kind == 38):
            self.Get( )
            is_true = False 
         if self.la.kind == 39:
            self.Get( )
            val = self.betweenExpr(arg, is_true)
         elif self.la.kind == 40:
            self.Get( )
            val = self.likeExpr(arg,False, is_true)
         elif self.la.kind == 41:
            self.Get( )
            self.Expect(42)
            val = self.likeExpr(arg,True, is_true)
         elif self.la.kind == 43:
            self.Get( )
            self.Expect(9)
            val = self.inExpr(arg, is_true)
            self.Expect(10)
         elif self.la.kind == 44:
            self.Get( )
            val = self.valueLitteral()
            val = ast.ContainingExpr(arg,val, is_true) 
         elif self.la.kind == 45:
            self.Get( )
            self.Expect(7)
            val = self.valueLitteral()
            val = ast.StartingExpr(arg, val, is_true) 
         else:
            self.SynErr(117)
      elif self.StartOf(13):
         val = self.compareExpr(arg)
      elif self.la.kind == 46:
         self.Get( )
         if (self.la.kind == 38):
            self.Get( )
            is_true = False 
         if self.la.kind == 19:
            self.Get( )
            self.Expect(14)
            val = self.valueLitteral()
            val = ast.DistinctFrom(is_true, arg, val) 
         elif self.StartOf(14):
            val = self.truthValue(is_true, arg)
         else:
            self.SynErr(118)
      else:
         self.SynErr(119)
      return val

   def betweenExpr( self, arg, is_true ):
      symmetric = False 
      if (self.la.kind == 51 or self.la.kind == 52):
         if self.la.kind == 51:
            self.Get( )
         else:
            self.Get( )
            symmetric = True 
      v1 = self.valueLitteral()
      self.Expect(37)
      v2 = self.valueLitteral()
      val = ast.BetweenExpr(symmetric, is_true, [arg, v1, v2]) 
      return val

   def likeExpr( self, arg,rex, is_true ):
      esc = None 
      pat = self.String()
      if (self.la.kind == 53):
         self.Get( )
         val = self.String()
         esc = val 
      val = ast.LikeExpr(arg, pat, rex, is_true, esc) 
      return val

   def inExpr( self, arg, is_true ):
      if self.StartOf(5):
         val = self.valueLitteral()
         vals = [val] 
         while self.la.kind == 11:
            self.Get( )
            val = self.valueLitteral()
            vals.append(val) 

         val = ast.InExpr(arg, is_true, vals) 
      elif self.la.kind == 12:
         self.selectColumnList()
      else:
         self.SynErr(120)
      return val

   def compareExpr( self, arg ):
      op = '==' 
      if self.la.kind == 54:
         self.Get( )
      elif self.StartOf(15):
         if self.la.kind == 55:
            self.Get( )
         elif self.la.kind == 56:
            self.Get( )
         elif self.la.kind == 57:
            self.Get( )
         else:
            self.Get( )
         op = self.token.val 
      elif self.la.kind == 59:
         self.Get( )
         op = '!=' 
      else:
         self.SynErr(121)
      if self.StartOf(5):
         v = self.valueLitteral()
         val = ast.CompareExpr(op, arg,v) 
      elif self.la.kind == 60 or self.la.kind == 61:
         if self.la.kind == 60:
            self.Get( )
         else:
            self.Get( )
         self.Expect(9)
         val = self.sqlselect()
         self.Expect(10)
      else:
         self.SynErr(122)
      return val

   def truthValue( self, is_true, arg ):
      v = None 
      if self.la.kind == 47:
         self.Get( )
         v = True 
      elif self.la.kind == 48:
         self.Get( )
         v = False 
      elif self.la.kind == 49:
         self.Get( )
      elif self.la.kind == 50:
         self.Get( )
      else:
         self.SynErr(123)
      val = ast.Check(arg,is_true, v) 
      return val

   def selectColumnList( self ):
      val = self.sqlselect()

   def nameList( self ):
      val = [] 
      id = self.Name()
      val.append(id) 
      while self.la.kind == 11:
         self.Get( )
         id = self.Name()
         val.append(id) 

      return val

   def standartFunction( self ):
      id, args = None, [] 
      if self.la.kind == 64:
         self.Get( )
         self.Expect(9)
         self.Expect(10)
         id = ast.SqlFunc.ROW_NUMBER 
      elif self.la.kind == 65:
         self.Get( )
         self.Expect(9)
         v = self.valueExpr()
         self.Expect(8)
         type = self.Type()
         self.Expect(10)
         id, args = ast.SqlFunc.CAST, [type, v] 
      elif self.la.kind == 66:
         self.Get( )
         self.Expect(9)
         v1 = self.valueExpr()
         self.Expect(11)
         v2 = self.valueExpr()
         self.Expect(10)
         id, args = ast.SqlFunc.NULLIF, [v1,v2] 
      elif self.la.kind == 67:
         self.Get( )
         self.Expect(9)
         args = self.procedureArgs()
         self.Expect(10)
         id = ast.SqlFunc.COALESCE; 
      elif self.la.kind == 68:
         self.Get( )
         self.Expect(9)
         v = self.valueExpr()
         self.Expect(11)
         fromidx = self.Int()
         id, args = ast.SqlFunc.SUBSTRING, [v, fromidx] 
         if (self.la.kind == 11):
            self.Get( )
            c = self.Int()
            args.append(c) 
         self.Expect(10)
      elif self.la.kind == 69 or self.la.kind == 70:
         if self.la.kind == 69:
            self.Get( )
            id = ast.SqlFunc.UPPER 
         else:
            self.Get( )
            id = ast.SqlFunc.LOWER 
         self.Expect(9)
         v = self.valueExpr()
         args = [v] 
         self.Expect(10)
      elif self.la.kind == 71:
         self.Get( )
         self.Expect(9)
         id, args = ast.SqlFunc.TRIM, [ast.Trim.BOTH]  
         if (self.la.kind == 72 or self.la.kind == 73 or self.la.kind == 74):
            if self.la.kind == 72:
               self.Get( )
               args[0] = ast.Trim.LEADING 
            elif self.la.kind == 73:
               self.Get( )
               args[0] = ast.Trim.TRAILING 
            else:
               self.Get( )
         v = self.valueExpr()
         args.append(v) 
         if (self.la.kind == 14):
            self.Get( )
            v = self.valueExpr()
            args.append(v) 
         self.Expect(10)
      elif self.la.kind == 75:
         self.Get( )
         self.Expect(9)
         v1 = self.valueExpr()
         self.Expect(76)
         v2 = self.valueExpr()
         self.Expect(14)
         p = self.Int()
         id, args = ast.SqlFunc.OVERLAY, [v1, v2, p] 
         if (self.la.kind == 77):
            self.Get( )
            l = self.Int()
            args.append(l) 
         self.Expect(10)
      else:
         self.SynErr(124)
      val = ast.SQLFunction(id, args) 
      return val

   def valueExpr( self ):
      op = None 
      val = self.term()
      while self.la.kind == 78 or self.la.kind == 79 or self.la.kind == 100:
         if self.la.kind == 78:
            self.Get( )
         elif self.la.kind == 79:
            self.Get( )
         else:
            self.Get( )
         op = self.token.val 
         v2 = self.term()
         val = ast.BinaryExpr(op,val,v2) 

      return val

   def Type( self ):
      if self.la.kind == 85 or self.la.kind == 86:
         if self.la.kind == 85:
            self.Get( )
         else:
            self.Get( )
         val = str 
      elif self.StartOf(16):
         if self.la.kind == 87:
            self.Get( )
         elif self.la.kind == 88:
            self.Get( )
         elif self.la.kind == 89:
            self.Get( )
         elif self.la.kind == 90:
            self.Get( )
         elif self.la.kind == 91:
            self.Get( )
         else:
            self.Get( )
         val = int 
      elif self.la.kind == 93 or self.la.kind == 94 or self.la.kind == 95:
         if self.la.kind == 93:
            self.Get( )
         elif self.la.kind == 94:
            self.Get( )
         else:
            self.Get( )
            self.Expect(96)
         val = float 
      elif self.la.kind == 97:
         self.Get( )
         val = bool 
      elif self.la.kind == 98:
         self.Get( )
         val = "DATE" 
      elif self.la.kind == 99:
         self.Get( )
         val = "DATETIME" 
      else:
         self.SynErr(125)
      return val

   def procedureArgs( self ):
      val = self.valueExpr()
      vals = [val] 
      while self.la.kind == 11:
         self.Get( )
         val = self.valueExpr()
         vals.append(val) 

      return vals

   def Int( self ):
      self.Expect(4)
      val = int(self.token.val) 
      return val

   def BoolLiteral( self ):
      if self.la.kind == 47:
         self.Get( )
         val = True 
      elif self.la.kind == 48:
         self.Get( )
         val = False 
      else:
         self.SynErr(126)
      return val

   def caseExpr( self ):
      if self.StartOf(5):
         val = self.simpleSwitch()
      elif self.la.kind == 83:
         val = self.searchedCase()
      else:
         self.SynErr(127)
      if (self.la.kind == 82):
         self.Get( )
         elval = self.caseresult()
         val.elsevalue = elval 
      return val

   def scalarSubquery( self ):
      val = self.sqlselect()
      return val

   def SQLParameter( self ):
      self.Expect(3)
      val = self.context.addParam(self.getCasesensitiveTokenValue(self.token)[1:]) 
      return val

   def simpleSwitch( self ):
      val, cases = None, [] 
      val = self.valueLitteral()
      self.Expect(83)
      self.simpleCase(cases)
      while self.la.kind == 83:
         self.Get( )
         self.simpleCase(cases)

      val = ast.SimpleSwitch(val, cases) 
      return val

   def searchedCase( self ):
      cases = [] 
      self.Expect(83)
      self.searchCase(cases)
      while self.la.kind == 83:
         self.Get( )
         self.searchCase(cases)

      val = ast.SearchedSwitch(cases) 
      return val

   def caseresult( self ):
      val = None 
      if self.StartOf(5):
         val = self.valueLitteral()
      elif self.la.kind == 50:
         self.Get( )
      else:
         self.SynErr(128)
      return val

   def simpleCase( self, cases ):
      if self.StartOf(5):
         ifv = self.valueLitteral()
      elif self.StartOf(11):
         ifv = self.compareOperand(None)
      else:
         self.SynErr(129)
      self.Expect(84)
      thenv = self.caseresult()
      cases.append(ast.SimpleCase(ifv, thenv))  

   def searchCase( self, cases ):
      v = self.searchCondition()
      self.Expect(84)
      r = self.caseresult()
      cases.append(ast.SearchCase(v, r))  

   def term( self ):
      op = None 
      val = self.factor()
      while self.la.kind == 13 or self.la.kind == 101 or self.la.kind == 102:
         if self.la.kind == 13:
            self.Get( )
         elif self.la.kind == 101:
            self.Get( )
         else:
            self.Get( )
         op = self.token.val 
         v2 = self.factor()
         val = ast.BinaryExpr(op,val,v2) 

      return val

   def factor( self ):
      val = self.valueLitteral()
      return val



   def Parse( self, scanner ):
      self.scanner = scanner
      self.la = Token( )
      self.la.val = ''
      self.Get( )
      self.sql()
      self.Expect(0)


   set = [
      [T,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,T,T,T, T,T,T,x, x,T,x,x, x,x,x,x, x,x,x,x, x,T,T,T, T,T,T,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,T, T,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, T,T,T,T, T,T,T,T, x,x,x,T, x,x,T,T, T,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,T, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, T,T,T,T, x,T,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, T,T,T,T, x,T,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, T,T,T,T, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,T,T,T, T,T,T,x, x,T,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,T, T,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, T,T,T,T, T,T,T,T, x,x,x,T, x,x,T,T, T,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,T,T,T, T,T,T,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, T,T,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,T,T, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, T,T,T,T, T,T,T,T, x,x,x,T, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,T,T,T, T,T,T,x, x,T,x,x, x,x,x,x, x,x,x,T, T,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,T, T,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, T,T,T,T, T,T,T,T, x,x,x,T, x,x,T,T, T,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,T,T, T,T,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,T,T, T,T,x,T, T,T,T,x, x,x,x,x, x,x,T,T, T,T,T,T, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,T,T, T,T,x,T, T,T,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,T,T, T,T,T,T, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,T, T,T,T,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,T, T,T,T,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x],
      [x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,x, x,x,x,T, T,T,T,T, T,x,x,x, x,x,x,x, x,x,x,x, x,x]

      ]

   errorMessages = {
      
      0 : "EOF expected",
      1 : "ident expected",
      2 : "fixident expected",
      3 : "paramId expected",
      4 : "integer expected",
      5 : "float expected",
      6 : "string expected",
      7 : "\"with\" expected",
      8 : "\"as\" expected",
      9 : "\"(\" expected",
      10 : "\")\" expected",
      11 : "\",\" expected",
      12 : "\"select\" expected",
      13 : "\"*\" expected",
      14 : "\"from\" expected",
      15 : "\"where\" expected",
      16 : "\"group\" expected",
      17 : "\"by\" expected",
      18 : "\"order\" expected",
      19 : "\"distinct\" expected",
      20 : "\"all\" expected",
      21 : "\"count\" expected",
      22 : "\"avg\" expected",
      23 : "\"max\" expected",
      24 : "\"min\" expected",
      25 : "\"sum\" expected",
      26 : "\"list\" expected",
      27 : "\"filter\" expected",
      28 : "\"inner\" expected",
      29 : "\"left\" expected",
      30 : "\"right\" expected",
      31 : "\"full\" expected",
      32 : "\"outer\" expected",
      33 : "\"join\" expected",
      34 : "\"on\" expected",
      35 : "\"using\" expected",
      36 : "\"or\" expected",
      37 : "\"and\" expected",
      38 : "\"not\" expected",
      39 : "\"between\" expected",
      40 : "\"like\" expected",
      41 : "\"similar\" expected",
      42 : "\"to\" expected",
      43 : "\"in\" expected",
      44 : "\"containing\" expected",
      45 : "\"starting\" expected",
      46 : "\"is\" expected",
      47 : "\"true\" expected",
      48 : "\"false\" expected",
      49 : "\"unknown\" expected",
      50 : "\"null\" expected",
      51 : "\"asymmetric\" expected",
      52 : "\"symmetric\" expected",
      53 : "\"escape\" expected",
      54 : "\"=\" expected",
      55 : "\"<\" expected",
      56 : "\">\" expected",
      57 : "\"<=\" expected",
      58 : "\">=\" expected",
      59 : "\"<>\" expected",
      60 : "\"some\" expected",
      61 : "\"any\" expected",
      62 : "\"asc\" expected",
      63 : "\"desc\" expected",
      64 : "\"row_number\" expected",
      65 : "\"cast\" expected",
      66 : "\"nullif\" expected",
      67 : "\"coalesce\" expected",
      68 : "\"substring\" expected",
      69 : "\"upper\" expected",
      70 : "\"lower\" expected",
      71 : "\"trim\" expected",
      72 : "\"leading\" expected",
      73 : "\"trailing\" expected",
      74 : "\"both\" expected",
      75 : "\"overlay\" expected",
      76 : "\"placing\" expected",
      77 : "\"for\" expected",
      78 : "\"+\" expected",
      79 : "\"-\" expected",
      80 : "\"case\" expected",
      81 : "\"end\" expected",
      82 : "\"else\" expected",
      83 : "\"when\" expected",
      84 : "\"then\" expected",
      85 : "\"character\" expected",
      86 : "\"char\" expected",
      87 : "\"numeric\" expected",
      88 : "\"decimal\" expected",
      89 : "\"dec\" expected",
      90 : "\"smallint\" expected",
      91 : "\"integer\" expected",
      92 : "\"int\" expected",
      93 : "\"float\" expected",
      94 : "\"real\" expected",
      95 : "\"double\" expected",
      96 : "\"precision\" expected",
      97 : "\"boolean\" expected",
      98 : "\"date\" expected",
      99 : "\"datetime\" expected",
      100 : "\"||\" expected",
      101 : "\"/\" expected",
      102 : "\"%\" expected",
      103 : "\".\" expected",
      104 : "??? expected",
      105 : "invalid sqlselect",
      106 : "invalid Ident",
      107 : "invalid setQuantifier",
      108 : "invalid tableRefList",
      109 : "invalid selectItem",
      110 : "invalid valueLitteral",
      111 : "invalid valueLitteral",
      112 : "invalid valueLitteral",
      113 : "invalid aggregateFunction",
      114 : "invalid aggregateFunction",
      115 : "invalid tableReference",
      116 : "invalid joinSpecification",
      117 : "invalid compareOperand",
      118 : "invalid compareOperand",
      119 : "invalid compareOperand",
      120 : "invalid inExpr",
      121 : "invalid compareExpr",
      122 : "invalid compareExpr",
      123 : "invalid truthValue",
      124 : "invalid standartFunction",
      125 : "invalid Type",
      126 : "invalid BoolLiteral",
      127 : "invalid caseExpr",
      128 : "invalid caseresult",
      129 : "invalid simpleCase",
      }


