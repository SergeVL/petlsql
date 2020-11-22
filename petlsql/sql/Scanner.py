

class Token( object ):
   def __init__( self ):
      self.kind   = 0     # token kind
      self.pos    = 0     # token position in the source text (starting at 0)
      self.col    = 0     # token column (starting at 0)
      self.line   = 0     # token line (starting at 1)
      self.val    = u''   # token value
      self.next   = None  # AW 2003-03-07 Tokens are kept in linked list


class Position( object ):    # position of source code stretch (e.g. semantic action, resolver expressions)
   def __init__( self, buf, beg, len, col ):
      assert isinstance( buf, Buffer )
      assert isinstance( beg, int )
      assert isinstance( len, int )
      assert isinstance( col, int )
      
      self.buf = buf
      self.beg = beg   # start relative to the beginning of the file
      self.len = len   # length of stretch
      self.col = col   # column number of start position

   def getSubstring( self ):
      return self.buf.readPosition( self )

class Buffer( object ):
   EOF      = u'\u0100'     # 256

   def __init__( self, s ):
      self.buf    = s
      self.bufLen = len(s)
      self.pos    = 0
      self.lines  = s.splitlines( True )

   def Read( self ):
      if self.pos < self.bufLen:
         result = self.buf[self.pos]
         self.pos += 1
         return result
      else:
         return Buffer.EOF


   def ReadChars( self, numBytes=1 ):
      result = self.buf[ self.pos : self.pos + numBytes ]
      self.pos += numBytes
      return result

   def Peek( self ):
      if self.pos < self.bufLen:
         return self.buf[self.pos]
      else:
         return Scanner.buffer.EOF

   def getSlice( self, beg, end ):
      return self.buf[ beg : end ]  

   def getString( self, beg, end ):
      s = ''
      oldPos = self.getPos( )
      self.setPos( beg )
      while beg < end:
         s += self.Read( )
         beg += 1
      self.setPos( oldPos )
      return s

   def getPos( self ):
      return self.pos

   def setPos( self, value ):
      if value < 0:
         self.pos = 0
      elif value >= self.bufLen:
         self.pos = self.bufLen
      else:
         self.pos = value

   def readPosition( self, pos ):
      assert isinstance( pos, Position )
      self.setPos( pos.beg )
      return self.ReadChars( pos.len )

   def __iter__( self ):
      return iter(self.lines)

class Scanner(object):
   EOL     = u'\n'
   eofSym  = 0

   charSetSize = 256
   maxT = 104
   noSym = 104
   start = [
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  8,  0,  2, 28,  0,  7, 15, 16, 18, 23, 17, 24, 29, 27,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10,  0,  0, 30, 19, 31,  4,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,
     0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
     1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0, 25,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
     -1]
   valCh = u''       # current input character (for token.val)

   def __init__( self, s ):
      self.buffer = Buffer( str(s) ) # the buffer instance

      self.ch        = u'\0'       # current input character
      self.pos       = -1          # column number of current character
      self.line      = 1           # line number of current character
      self.lineStart = 0           # start position of current line
      self.oldEols   = 0           # EOLs that appeared in a comment;
      self.NextCh( )
      self.ignore    = set( )      # set of characters to be ignored by the scanner
      self.ignore.add( ord(' ') )  # blanks are always white space
      self.ignore.add(9) 
      self.ignore.add(10) 
      self.ignore.add(13) 

      self.token = self.pt = Token()

   def NextCh( self ):
      if self.oldEols > 0:
         self.ch = Scanner.EOL
         self.oldEols -= 1
      else:
         self.ch = self.buffer.Read( )
         self.pos += 1
         # replace isolated '\r' by '\n' in order to make
         # eol handling uniform across Windows, Unix and Mac
         if (self.ch == u'\r') and (self.buffer.Peek() != u'\n'):
            self.ch = Scanner.EOL
         if self.ch == Scanner.EOL:
            self.line += 1
            self.lineStart = self.pos + 1
            valCh = self.ch
      if self.ch != Buffer.EOF:
         self.ch = self.ch.lower()



   def Comment0(self):
      level = 1
      line0 = self.line
      lineStart0 = self.lineStart
      self.NextCh()
      if self.ch == '*':
         self.NextCh()
         while True:
            if self.ch == '*':
               self.NextCh()
               if self.ch == '/':
                  level -= 1
                  if level == 0:
                     self.oldEols = self.line - line0
                     self.NextCh()
                     return True
                  self.NextCh()
            elif self.ch == Buffer.EOF:
               return False
            else:
               self.NextCh()
      else:
         if self.ch == Scanner.EOL:
            self.line -= 1
            self.lineStart = lineStart0
         self.pos = self.pos - 2
         self.buffer.setPos(self.pos+1)
         self.NextCh()
      return False

   def Comment1(self):
      level = 1
      line0 = self.line
      lineStart0 = self.lineStart
      self.NextCh()
      if self.ch == '/':
         self.NextCh()
         while True:
            if ord(self.ch) == 10:
               level -= 1
               if level == 0:
                  self.oldEols = self.line - line0
                  self.NextCh()
                  return True
               self.NextCh()
            elif self.ch == Buffer.EOF:
               return False
            else:
               self.NextCh()
      else:
         if self.ch == Scanner.EOL:
            self.line -= 1
            self.lineStart = lineStart0
         self.pos = self.pos - 2
         self.buffer.setPos(self.pos+1)
         self.NextCh()
      return False


   def CheckLiteral( self ):
      lit = self.t.val.lower()
      if lit == "with":
         self.t.kind = 7
      elif lit == "as":
         self.t.kind = 8
      elif lit == "select":
         self.t.kind = 12
      elif lit == "from":
         self.t.kind = 14
      elif lit == "where":
         self.t.kind = 15
      elif lit == "group":
         self.t.kind = 16
      elif lit == "by":
         self.t.kind = 17
      elif lit == "order":
         self.t.kind = 18
      elif lit == "distinct":
         self.t.kind = 19
      elif lit == "all":
         self.t.kind = 20
      elif lit == "count":
         self.t.kind = 21
      elif lit == "avg":
         self.t.kind = 22
      elif lit == "max":
         self.t.kind = 23
      elif lit == "min":
         self.t.kind = 24
      elif lit == "sum":
         self.t.kind = 25
      elif lit == "list":
         self.t.kind = 26
      elif lit == "filter":
         self.t.kind = 27
      elif lit == "inner":
         self.t.kind = 28
      elif lit == "left":
         self.t.kind = 29
      elif lit == "right":
         self.t.kind = 30
      elif lit == "full":
         self.t.kind = 31
      elif lit == "outer":
         self.t.kind = 32
      elif lit == "join":
         self.t.kind = 33
      elif lit == "on":
         self.t.kind = 34
      elif lit == "using":
         self.t.kind = 35
      elif lit == "or":
         self.t.kind = 36
      elif lit == "and":
         self.t.kind = 37
      elif lit == "not":
         self.t.kind = 38
      elif lit == "between":
         self.t.kind = 39
      elif lit == "like":
         self.t.kind = 40
      elif lit == "similar":
         self.t.kind = 41
      elif lit == "to":
         self.t.kind = 42
      elif lit == "in":
         self.t.kind = 43
      elif lit == "containing":
         self.t.kind = 44
      elif lit == "starting":
         self.t.kind = 45
      elif lit == "is":
         self.t.kind = 46
      elif lit == "true":
         self.t.kind = 47
      elif lit == "false":
         self.t.kind = 48
      elif lit == "unknown":
         self.t.kind = 49
      elif lit == "null":
         self.t.kind = 50
      elif lit == "asymmetric":
         self.t.kind = 51
      elif lit == "symmetric":
         self.t.kind = 52
      elif lit == "escape":
         self.t.kind = 53
      elif lit == "some":
         self.t.kind = 60
      elif lit == "any":
         self.t.kind = 61
      elif lit == "asc":
         self.t.kind = 62
      elif lit == "desc":
         self.t.kind = 63
      elif lit == "row_number":
         self.t.kind = 64
      elif lit == "cast":
         self.t.kind = 65
      elif lit == "nullif":
         self.t.kind = 66
      elif lit == "coalesce":
         self.t.kind = 67
      elif lit == "substring":
         self.t.kind = 68
      elif lit == "upper":
         self.t.kind = 69
      elif lit == "lower":
         self.t.kind = 70
      elif lit == "trim":
         self.t.kind = 71
      elif lit == "leading":
         self.t.kind = 72
      elif lit == "trailing":
         self.t.kind = 73
      elif lit == "both":
         self.t.kind = 74
      elif lit == "overlay":
         self.t.kind = 75
      elif lit == "placing":
         self.t.kind = 76
      elif lit == "for":
         self.t.kind = 77
      elif lit == "case":
         self.t.kind = 80
      elif lit == "end":
         self.t.kind = 81
      elif lit == "else":
         self.t.kind = 82
      elif lit == "when":
         self.t.kind = 83
      elif lit == "then":
         self.t.kind = 84
      elif lit == "character":
         self.t.kind = 85
      elif lit == "char":
         self.t.kind = 86
      elif lit == "numeric":
         self.t.kind = 87
      elif lit == "decimal":
         self.t.kind = 88
      elif lit == "dec":
         self.t.kind = 89
      elif lit == "smallint":
         self.t.kind = 90
      elif lit == "integer":
         self.t.kind = 91
      elif lit == "int":
         self.t.kind = 92
      elif lit == "float":
         self.t.kind = 93
      elif lit == "real":
         self.t.kind = 94
      elif lit == "double":
         self.t.kind = 95
      elif lit == "precision":
         self.t.kind = 96
      elif lit == "boolean":
         self.t.kind = 97
      elif lit == "date":
         self.t.kind = 98
      elif lit == "datetime":
         self.t.kind = 99


   def NextToken( self ):
      while ord(self.ch) in self.ignore:
         self.NextCh( )
      if (self.ch == '/' and self.Comment0() or self.ch == '/' and self.Comment1()):
         return self.NextToken()

      self.t = Token( )
      self.t.pos = self.pos
      self.t.col = self.pos - self.lineStart + 1
      self.t.line = self.line
      b = ord(self.ch)
      state = self.start[b] if b<=256 else 0
      buf = u''
      buf += self.ch
      self.NextCh()

      done = False
      while not done:
         if state == -1:
            self.t.kind = Scanner.eofSym     # NextCh already done
            done = True
         elif state == 0:
            self.t.kind = Scanner.noSym      # NextCh already done
            done = True
         elif state == 1:
            if (self.ch >= '0' and self.ch <= '9'
                 or self.ch == '_'
                 or self.ch >= 'a' and self.ch <= 'z'):
               buf += self.ch
               self.NextCh()
               state = 1
            else:
               self.t.kind = 1
               self.t.val = buf
               self.CheckLiteral()
               return self.t
         elif state == 2:
            if (self.ch == '_'
                 or self.ch >= 'a' and self.ch <= 'z'):
               buf += self.ch
               self.NextCh()
               state = 3
            else:
               self.t.kind = Scanner.noSym
               done = True
         elif state == 3:
            if (self.ch >= '0' and self.ch <= '9'
                 or self.ch == '_'
                 or self.ch >= 'a' and self.ch <= 'z'):
               buf += self.ch
               self.NextCh()
               state = 3
            else:
               self.t.kind = 2
               done = True
         elif state == 4:
            if (self.ch == '_'
                 or self.ch >= 'a' and self.ch <= 'z'):
               buf += self.ch
               self.NextCh()
               state = 5
            else:
               self.t.kind = Scanner.noSym
               done = True
         elif state == 5:
            if (self.ch >= '0' and self.ch <= '9'
                 or self.ch == '_'
                 or self.ch >= 'a' and self.ch <= 'z'):
               buf += self.ch
               self.NextCh()
               state = 5
            else:
               self.t.kind = 3
               done = True
         elif state == 6:
            if (self.ch >= '0' and self.ch <= '9'):
               buf += self.ch
               self.NextCh()
               state = 6
            else:
               self.t.kind = 5
               done = True
         elif state == 7:
            if (ord(self.ch) <= 9
                 or ord(self.ch) >= 11 and ord(self.ch) <= 12
                 or ord(self.ch) >= 14 and self.ch <= '&'
                 or self.ch >= '(' and self.ch <= '['
                 or self.ch >= ']' and ord(self.ch) <= 255 or ord(self.ch) > 256):
               buf += self.ch
               self.NextCh()
               state = 7
            elif ord(self.ch) == 39:
               buf += self.ch
               self.NextCh()
               state = 9
            elif ord(self.ch) == 92:
               buf += self.ch
               self.NextCh()
               state = 11
            else:
               self.t.kind = Scanner.noSym
               done = True
         elif state == 8:
            if (ord(self.ch) <= 9
                 or ord(self.ch) >= 11 and ord(self.ch) <= 12
                 or ord(self.ch) >= 14 and self.ch <= '!'
                 or self.ch >= '#' and self.ch <= '['
                 or self.ch >= ']' and ord(self.ch) <= 255 or ord(self.ch) > 256):
               buf += self.ch
               self.NextCh()
               state = 8
            elif self.ch == '"':
               buf += self.ch
               self.NextCh()
               state = 9
            elif ord(self.ch) == 92:
               buf += self.ch
               self.NextCh()
               state = 12
            else:
               self.t.kind = Scanner.noSym
               done = True
         elif state == 9:
            self.t.kind = 6
            done = True
         elif state == 10:
            if (self.ch >= '0' and self.ch <= '9'):
               buf += self.ch
               self.NextCh()
               state = 10
            elif self.ch == '.':
               buf += self.ch
               self.NextCh()
               state = 6
            else:
               self.t.kind = 4
               done = True
         elif state == 11:
            if (ord(self.ch) <= 9
                 or ord(self.ch) >= 11 and ord(self.ch) <= 12
                 or ord(self.ch) >= 14 and self.ch <= '&'
                 or self.ch >= '(' and self.ch <= '['
                 or self.ch >= ']' and ord(self.ch) <= 255 or ord(self.ch) > 256):
               buf += self.ch
               self.NextCh()
               state = 7
            elif ord(self.ch) == 39:
               buf += self.ch
               self.NextCh()
               state = 13
            elif ord(self.ch) == 92:
               buf += self.ch
               self.NextCh()
               state = 11
            else:
               self.t.kind = Scanner.noSym
               done = True
         elif state == 12:
            if (ord(self.ch) <= 9
                 or ord(self.ch) >= 11 and ord(self.ch) <= 12
                 or ord(self.ch) >= 14 and self.ch <= '!'
                 or self.ch >= '#' and self.ch <= '['
                 or self.ch >= ']' and ord(self.ch) <= 255 or ord(self.ch) > 256):
               buf += self.ch
               self.NextCh()
               state = 8
            elif self.ch == '"':
               buf += self.ch
               self.NextCh()
               state = 14
            elif ord(self.ch) == 92:
               buf += self.ch
               self.NextCh()
               state = 12
            else:
               self.t.kind = Scanner.noSym
               done = True
         elif state == 13:
            if (ord(self.ch) <= 9
                 or ord(self.ch) >= 11 and ord(self.ch) <= 12
                 or ord(self.ch) >= 14 and self.ch <= '&'
                 or self.ch >= '(' and self.ch <= '['
                 or self.ch >= ']' and ord(self.ch) <= 255 or ord(self.ch) > 256):
               buf += self.ch
               self.NextCh()
               state = 7
            elif ord(self.ch) == 39:
               buf += self.ch
               self.NextCh()
               state = 9
            elif ord(self.ch) == 92:
               buf += self.ch
               self.NextCh()
               state = 11
            else:
               self.t.kind = 6
               done = True
         elif state == 14:
            if (ord(self.ch) <= 9
                 or ord(self.ch) >= 11 and ord(self.ch) <= 12
                 or ord(self.ch) >= 14 and self.ch <= '!'
                 or self.ch >= '#' and self.ch <= '['
                 or self.ch >= ']' and ord(self.ch) <= 255 or ord(self.ch) > 256):
               buf += self.ch
               self.NextCh()
               state = 8
            elif self.ch == '"':
               buf += self.ch
               self.NextCh()
               state = 9
            elif ord(self.ch) == 92:
               buf += self.ch
               self.NextCh()
               state = 12
            else:
               self.t.kind = 6
               done = True
         elif state == 15:
            self.t.kind = 9
            done = True
         elif state == 16:
            self.t.kind = 10
            done = True
         elif state == 17:
            self.t.kind = 11
            done = True
         elif state == 18:
            self.t.kind = 13
            done = True
         elif state == 19:
            self.t.kind = 54
            done = True
         elif state == 20:
            self.t.kind = 57
            done = True
         elif state == 21:
            self.t.kind = 58
            done = True
         elif state == 22:
            self.t.kind = 59
            done = True
         elif state == 23:
            self.t.kind = 78
            done = True
         elif state == 24:
            self.t.kind = 79
            done = True
         elif state == 25:
            if self.ch == '|':
               buf += self.ch
               self.NextCh()
               state = 26
            else:
               self.t.kind = Scanner.noSym
               done = True
         elif state == 26:
            self.t.kind = 100
            done = True
         elif state == 27:
            self.t.kind = 101
            done = True
         elif state == 28:
            self.t.kind = 102
            done = True
         elif state == 29:
            self.t.kind = 103
            done = True
         elif state == 30:
            if self.ch == '=':
               buf += self.ch
               self.NextCh()
               state = 20
            elif self.ch == '>':
               buf += self.ch
               self.NextCh()
               state = 22
            else:
               self.t.kind = 55
               done = True
         elif state == 31:
            if self.ch == '=':
               buf += self.ch
               self.NextCh()
               state = 21
            else:
               self.t.kind = 56
               done = True

      self.t.val = buf
      return self.t

   def Scan( self ):
      if self.token.next is None:
         self.pt = self.token = self.NextToken( )
      else:
         self.pt = self.token = self.token.next
      return self.token

   def Peek( self ):
      if self.pt.next is None:
          self.pt.next = self.NextToken()
      self.pt = self.pt.next
      while self.pt.kind > self.maxT:
        if self.pt.next is None:
          self.pt.next = self.NextToken()
        self.pt = self.pt.next
      return self.pt

   def ResetPeek( self ):
      self.pt = self.token


