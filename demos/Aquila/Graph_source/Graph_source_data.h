static int _comment_      = 0;
static int _forum_        = 1;
static int _person_       = 2;
static int _post_         = 3;
static int _organisation_ = 4;
static int _place_        = 5;
static int _tag_          = 6;
static int _tagclass_     = 7;

static int _comment_hasCreator_person_        = 0;
static int _comment_hasTag_tag_               = 1;
static int _comment_isLocatedIn_place_        = 2;
static int _comment_replyOf_comment_          = 3;
static int _comment_replyOf_post_             = 4;
static int _forum_containerOf_post_           = 5;
static int _forum_hasMember_person_           = 6;
static int _forum_hasModerator_person_        = 7;
static int _forum_hasTag_tag_                 = 8;
static int _person_hasInterest_tag_           = 9;
static int _person_isLocatedIn_place_         = 10;
static int _person_knows_person_              = 11;
static int _person_likes_comment_             = 12;
static int _person_likes_post_                = 13;
static int _person_studyAt_organisation_      = 14;
static int _person_workAt_organisation_       = 15;
static int _post_hasCreator_person_           = 16;
static int _post_hasTag_tag_                  = 17;
static int _post_isLocatedIn_place_           = 18;
static int _organisation_isLocatedIn_place_   = 19;
static int _place_isPartOf_place_             = 20;
static int _tag_hasType_tagclass_             = 21;
static int _tagclass_isSubclassOf_tagclass_   = 22;

static int _person_hasCreated_post_           = 23;
static int _person_hasCreated_comment_        = 24;
static int _post_beliked_person_             = 25;
static int _post_hasReply_comment_            = 26;
static int _tag_beinterested_person          = 27;
static int _tagclass_hasSubclass_tagclass_    = 28;

static long long max_distance = 999999999999;

static std::unordered_map<std::string, int> string_to_index = {
        {"comment", _comment_},
        {"forum", _forum_},
        {"person", _person_},
        {"destination_person", _person_},
        {"post", _post_},
        {"organisation", _organisation_},
        {"place", _place_},
        {"tag", _tag_},
        {"tagclass", _tagclass_},
        {"comment-[hasCreator]->person", _comment_hasCreator_person_},
        {"comment-[hasTag]->tag", _comment_hasTag_tag_},
        {"comment-[isLocatedIn]->place", _comment_isLocatedIn_place_},
        {"comment-[replyOf]->comment", _comment_replyOf_comment_},
        {"comment-[replyOf]->post", _comment_replyOf_post_},
        {"forum-[containerOf]->post", _forum_containerOf_post_},
        {"forum-[hasMember]->person", _forum_hasMember_person_},
        {"forum-[hasModerator]->person", _forum_hasModerator_person_},
        {"forum-[hasTag]->tag", _forum_hasTag_tag_},
        {"person-[hasInterest]->tag", _person_hasInterest_tag_},
        {"person-[isLocatedIn]->place", _person_isLocatedIn_place_},
        {"person-[knows]->person", _person_knows_person_},
        {"person-[likes]->comment", _person_likes_comment_},
        {"person-[likes]->post", _person_likes_post_},
        {"person-[studyAt]->organisation", _person_studyAt_organisation_},
        {"person-[workAt]->organisation", _person_workAt_organisation_},
        {"post-[hasCreator]->person", _post_hasCreator_person_},
        {"post-[hasTag]->tag", _post_hasTag_tag_},
        {"post-[isLocatedIn]->place", _post_isLocatedIn_place_},
        {"organisation-[isLocatedIn]->place", _organisation_isLocatedIn_place_},
        {"place-[isPartOf]->place", _place_isPartOf_place_},
        {"tag-[hasType]->tagclass", _tag_hasType_tagclass_},
        {"tagclass-[isSubclassOf]->tagclass", _tagclass_isSubclassOf_tagclass_},
        {"person-[hasCreated]->post", _person_hasCreated_post_},
        {"person-[hasCreated]->comment", _person_hasCreated_comment_},
        {"post-[beliked]->person", _post_beliked_person_},
        {"post-[hasReply]->comment", _post_hasReply_comment_},
        {"tag-[beinterested]->person", _tag_beinterested_person},
        {"tagclass-[hasSubclass]->tagclass", _tagclass_hasSubclass_tagclass_}
};