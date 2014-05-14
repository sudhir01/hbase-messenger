package com.myernore.e68.hbasemessenger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import com.myernore.e68.hbasemessenger.hbase.UsersDAO;
import com.myernore.e68.hbasemessenger.model.User;


public class LoadMessages {

	public static final String usage = "loadmessages loadRomeoAndJuliet\n"
			+ "  help                                       :  print this message and exit.\n"
         + "  loadRomeoAndJuliet                         :  loads Romeo, Juliet, Nurse Act 2 Scene 2 lines\n";

	public static void main(String[] args) throws IOException {
		if (args.length < 1 || "help".equals(args[0])) {
			System.out.println(usage);
			System.exit(0);
		}
		if( args[0].equals("loadRomeoAndJuliet") ) {
		   loadRomeoAndJulietMessages();
		}
	}

	public static void loadRomeoAndJulietMessages() throws IOException {
	   HTablePool pool = new HTablePool();
      UsersDAO usersDao = new UsersDAO(pool);
      for(String[] line : rAndJ ) {
         usersDao.addMessage(line[0], line[1], line[2]);
      }
      pool.closeTablePool(UsersDAO.TABLE_NAME);
   }

   static String[][] rAndJ = new String[][] {
         {"Romeo", "Juliet", "If I profane with my unworthiest hand\nThis holy shrine, the gentle fine is this:\nMy lips, two blushing pilgrims, ready stand\nTo smooth that rough touch with a tender kiss."},
         {"Juliet", "Romeo", "Good pilgrim, you do wrong your hand too much,\nWhich mannerly devotion shows in this;\nFor saints have hands that pilgrims' hands do touch,\nAnd palm to palm is holy palmers' kiss."},
         {"Romeo", "Juliet", "Have not saints lips, and holy palmers too?"},
         {"Juliet", "Romeo", "Ay, pilgrim, lips that they must use in prayer."},
         {"Romeo", "Juliet", "O, then, dear saint, let lips do what hands do;\nThey pray, grant thou, lest faith turn to despair."},
         {"Juliet", "Romeo", "Saints do not move, though grant for prayers' sake."},
         {"Romeo", "Juliet", "Then move not, while my prayer's effect I take.\nThus from my lips, by yours, my sin is purged."},
         {"Juliet", "Romeo", "Then have my lips the sin that they have took."},
         {"Romeo", "Juliet", "Sin from thy lips? O trespass sweetly urged!\nGive me my sin again."},
         {"Juliet", "Romeo", "You kiss by the book."},
         {"Nurse", "Juliet", "Madam, your mother craves a word with you."},
         {"Romeo", "Nurse", "What is her mother?"},
         {"Nurse", "Romeo", "Marry, bachelor,\nHer mother is the lady of the house,\nAnd a good lady, and a wise and virtuous\nI nursed her daughter, that you talk'd withal;\nI tell you, he that can lay hold of her\nShall have the chinks."},
         {"Romeo", "Romeo", "Is she a Capulet?\nO dear account! my life is my foe's debt."},
         {"Juliet", "Nurse", "Go ask his name: if he be married.\nMy grave is like to be my wedding bed."},
         {"Nurse", "Juliet", "His name is Romeo, and a Montague;\nThe only son of your great enemy."},
         {"Juliet", "Nurse", "My only love sprung from my only hate!\nToo early seen unknown, and known too late!\nProdigious birth of love it is to me,\nThat I must love a loathed enemy."},
         {"Nurse", "Juliet", "What's this? what's this?"},
         {"Juliet", "Nurse", "A rhyme I learn'd even now\nOf one I danced withal."},
         {"Nurse", "Juliet", "Anon, anon!\nCome, let's away; the strangers all are gone."}
   };
}
