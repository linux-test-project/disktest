/*
* Disktest
* Copyright (c) International Business Machines Corp., 2001
*
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program; if not, write to the Free Software
* Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*
*  Please send e-mail to yardleyb@us.ibm.com if you have
*  questions or comments.
*
*  Project Website:  TBD
*
* $Id: globals.h,v 1.7.2.1 2009/01/21 00:53:26 yardleyb Exp $
*
*/

#ifndef _GLOBALS_H
#define _GLOBALS_H 1

#include "defs.h"
#include "threading.h"

/* global flags */
#define GLB_FLG_QUIET	0x00000001
#define GLB_FLG_SUPRESS	0x00000002
#define GLB_FLG_PERFP	0x00000004 /* forces alternate performance printing format */
#define GLB_FLG_KILL	0x00000008 /* will kill all threads to all targets when set */
#define GLB_FLG_FAILED	0x00000010 /* will kill all threads to all targets when set */

#define PDBG1  if(gbl_dbg_lvl > 0) pMsg
#define PDBG2  if(gbl_dbg_lvl > 1) pMsg
#define PDBG3  if(gbl_dbg_lvl > 2) pMsg
#define PDBG4  if(gbl_dbg_lvl > 3) pMsg
#define PDBG5  if(gbl_dbg_lvl > 4) pMsg

extern unsigned int gbl_dbg_lvl;

void init_gbl_data(test_env_t *);
#ifdef WINDOWS
void PrintLastSystemError(unsigned long);
void GetSystemErrorString(unsigned long, void *);
#endif

#endif /* _GLOBALS_H */

