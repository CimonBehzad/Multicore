
\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{forsetedocument}[2008/12/31]

\LoadClass[]{article}  %% twoside

\RequirePackage[charter]{mathdesign}
\RequirePackage{microtype}
\RequirePackage{bera}
\RequirePackage{listings}
\RequirePackage[version=latest]{pgf}
\RequirePackage{tikz} 
\RequirePackage{makeidx}
\RequirePackage{multicol}
\RequirePackage{fleqn}
\RequirePackage{array}
\RequirePackage{latexsym}
\RequirePackage{verbatim}
\RequirePackage{latexsym}

\lstset{
  language=C,
  xleftmargin=7mm,
  tabsize=8,
  numberstyle=\small,
  stepnumber=1,
  numbersep=5pt,
  basicstyle=\ttfamily,
  keywordstyle=\ttfamily,  %% \bfseries,
  identifierstyle=,
  commentstyle=\it,
  stringstyle=\ttfamily,
  showstringspaces=false,
  otherkeywords={_Thread_local,inline, _Bool, _Complex, _Imaginary, restrict,complex,imaginary,_Alignas,_Noreturn,alignof,_Atomic,_Static_assert,_Generic,vector},
}

\newcommand{\cinline}[1]{\lstinline[language=C]§#1§}

\lstnewenvironment{ccode}{
  \lstset{
    language=C,
    xleftmargin=4mm,
    tabsize=8,
    numberstyle=\small,
    stepnumber=1,
    numbersep=5pt,
    basicstyle=\ttfamily,
    identifierstyle=,
    commentstyle=\it,
    stringstyle=\ttfamily,
    showstringspaces=false
%morekeywords={_Alignas,_Noreturn,%
%alignof,_Atomic,_Thread_local,_Static_assert,_Generic},
  }
}
{}
\lstnewenvironment{ccodex}[1][]{
  \lstset{
    language=C,
    xleftmargin=7mm,
    tabsize=8,
    numberstyle=\small,
    stepnumber=1,
    numbersep=5pt,
    basicstyle=\ttfamily,
    keywordstyle=\bfseries,
    identifierstyle=,
    commentstyle=\it,
    stringstyle=\ttfamily,
    showstringspaces=false,
    #1
  }
}
{}
